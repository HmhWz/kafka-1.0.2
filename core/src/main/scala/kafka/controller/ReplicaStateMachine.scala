/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/
package kafka.controller

import kafka.common.{StateChangeFailedException, TopicAndPartition}
import kafka.controller.Callbacks.CallbackBuilder
import kafka.utils.{Logging, ReplicationUtils}

import scala.collection._

/**
 * This class represents the state machine for replicas. It defines the states that a replica can be in, and
 * transitions to move the replica to another legal state. The different states that a replica can be in are -
 * 1. NewReplica        : The controller can create new replicas during partition reassignment. In this state, a
 *                        replica can only get become follower state change request.  Valid previous
 *                        state is NonExistentReplica
 * 2. OnlineReplica     : Once a replica is started and part of the assigned replicas for its partition, it is in this
 *                        state. In this state, it can get either become leader or become follower state change requests.
 *                        Valid previous state are NewReplica, OnlineReplica or OfflineReplica
 * 3. OfflineReplica    : If a replica dies, it moves to this state. This happens when the broker hosting the replica
 *                        is down. Valid previous state are NewReplica, OnlineReplica
 * 4. ReplicaDeletionStarted: If replica deletion starts, it is moved to this state. Valid previous state is OfflineReplica
 * 5. ReplicaDeletionSuccessful: If replica responds with no error code in response to a delete replica request, it is
 *                        moved to this state. Valid previous state is ReplicaDeletionStarted
 * 6. ReplicaDeletionIneligible: If replica deletion fails, it is moved to this state. Valid previous state is ReplicaDeletionStarted
 * 7. NonExistentReplica: If a replica is deleted successfully, it is moved to this state. Valid previous state is
 *                        ReplicaDeletionSuccessful
 */
class ReplicaStateMachine(controller: KafkaController, stateChangeLogger: StateChangeLogger) extends Logging {

  private val controllerContext = controller.controllerContext
  private val controllerId = controller.config.brokerId
  private val zkUtils = controllerContext.zkUtils
  //副本状态机的缓存
  private val replicaState: mutable.Map[PartitionAndReplica, ReplicaState] = mutable.Map.empty
  private val brokerRequestBatch = new ControllerBrokerRequestBatch(controller, stateChangeLogger)

  this.logIdent = s"[ReplicaStateMachine controllerId=$controllerId] "


  /**
   * Invoked on successful controller election. First registers a broker change listener since that triggers all
   * state transitions for replicas. Initializes the state of replicas for all partitions by reading from zookeeper.
   * Then triggers the OnlineReplica state change for all replicas.
   */
  def startup() {
    // //初始化所有副本的状态信息
    initializeReplicaState()
    //将online的replica状态转变为OnlineReplica
    handleStateChanges(controllerContext.allLiveReplicas(), OnlineReplica)

    info("Started replica state machine with initial state -> " + replicaState.toString())
  }

  /**
   * Invoked on controller shutdown.
   */
  def shutdown() {
    replicaState.clear()

    info("Stopped replica state machine")
  }

  /**
   * This API is invoked by the broker change controller callbacks and the startup API of the state machine
   * @param replicas     The list of replicas (brokers) that need to be transitioned to the target state
   * @param targetState  The state that the replicas should be moved to
   * The controller's allLeaders cache should have been updated before this
   */
    //用于处理 Replica 状态的变化
  def handleStateChanges(replicas: Set[PartitionAndReplica], targetState: ReplicaState,
                         callbacks: Callbacks = (new CallbackBuilder).build) {
    if (replicas.nonEmpty) {
      info("Invoking state change to %s for replicas %s".format(targetState, replicas.mkString(",")))
      try {
        brokerRequestBatch.newBatch()
        //状态转变
        replicas.foreach(r => handleStateChange(r, targetState, callbacks))
        //向 broker 发送相应请求
        brokerRequestBatch.sendRequestsToBrokers(controller.epoch)
      } catch {
        case e: Throwable => error("Error while moving some replicas to %s state".format(targetState), e)
      }
    }
  }

  /**
   * This API exercises the replica's state machine. It ensures that every state transition happens from a legal
   * previous state to the target state. Valid state transitions are:
   * NonExistentReplica --> NewReplica
   * --send LeaderAndIsr request with current leader and isr to the new replica and UpdateMetadata request for the
   *   partition to every live broker
   *
   * NewReplica -> OnlineReplica
   * --add the new replica to the assigned replica list if needed
   *
   * OnlineReplica,OfflineReplica -> OnlineReplica
   * --send LeaderAndIsr request with current leader and isr to the new replica and UpdateMetadata request for the
   *   partition to every live broker
   *
   * NewReplica,OnlineReplica,OfflineReplica,ReplicaDeletionIneligible -> OfflineReplica
   * --send StopReplicaRequest to the replica (w/o deletion)
   * --remove this replica from the isr and send LeaderAndIsr request (with new isr) to the leader replica and
   *   UpdateMetadata request for the partition to every live broker.
   *
   * OfflineReplica -> ReplicaDeletionStarted
   * --send StopReplicaRequest to the replica (with deletion)
   *
   * ReplicaDeletionStarted -> ReplicaDeletionSuccessful
   * -- mark the state of the replica in the state machine
   *
   * ReplicaDeletionStarted -> ReplicaDeletionIneligible
   * -- mark the state of the replica in the state machine
   *
   * ReplicaDeletionSuccessful -> NonExistentReplica
   * -- remove the replica from the in memory partition replica assignment cache


   * @param partitionAndReplica The replica for which the state transition is invoked
   * @param targetState The end state that the replica should be moved to
   */
  def handleStateChange(partitionAndReplica: PartitionAndReplica, targetState: ReplicaState,
                        callbacks: Callbacks) {
    val topic = partitionAndReplica.topic
    val partition = partitionAndReplica.partition
    val replicaId = partitionAndReplica.replica
    val topicAndPartition = TopicAndPartition(topic, partition)
    // Replica 不存在的话,状态初始化为 NonExistentReplica
    val currState = replicaState.getOrElseUpdate(partitionAndReplica, NonExistentReplica)
    val stateChangeLog = stateChangeLogger.withControllerEpoch(controller.epoch)
    try {

      def logStateChange(): Unit =
        stateChangeLog.trace(s"Changed state of replica $replicaId for partition $topicAndPartition from " +
          s"$currState to $targetState")

      val replicaAssignment = controllerContext.partitionReplicaAssignment(topicAndPartition)
      //校验状态转变是否符合要求
      assertValidTransition(partitionAndReplica, targetState)
      targetState match {
        case NewReplica => ////其前置状态只能为 NonExistentReplica
          // start replica as a follower to the current leader for its partition
          //从 zk 获取 Partition 的 leaderAndIsr 信息
          val leaderIsrAndControllerEpochOpt = ReplicationUtils.getLeaderIsrAndEpochForPartition(zkUtils, topic, partition)
          leaderIsrAndControllerEpochOpt match {
            case Some(leaderIsrAndControllerEpoch) =>
              //若是leader的replica状态不能变为NewReplica
              if(leaderIsrAndControllerEpoch.leaderAndIsr.leader == replicaId)
                throw new StateChangeFailedException(s"Replica $replicaId for partition $topicAndPartition cannot " +
                  s"be moved to NewReplica state as it is being requested to become leader")
              //向该 replicaId 发送 LeaderAndIsr 请求,这个方法同时也会向所有的 broker 发送 updateMeta 请求
              brokerRequestBatch.addLeaderAndIsrRequestForBrokers(List(replicaId),
                                                                  topic, partition, leaderIsrAndControllerEpoch,
                                                                  replicaAssignment, isNew = true)
              //对于新建的 Partition，处于这个状态时，该 Partition 是没有相应的 LeaderAndIsr 信息的
            case None => // new leader request will be sent to this replica when one gets elected
          }
          //将该 Replica 的状态转移成 NewReplica，然后结束流程。
          replicaState.put(partitionAndReplica, NewReplica)
          logStateChange()
        case ReplicaDeletionStarted => //其前置状态只能为 OfflineReplica
          //更新向该 Replica 的状态为 ReplicaDeletionStarted；
          replicaState.put(partitionAndReplica, ReplicaDeletionStarted)
          // send stop replica command
          //发送 StopReplica 请求给该副本,并设置 deletePartition=true
          //broker收到这请求后，会从物理存储上删除这个 Replica 的数据内容
          brokerRequestBatch.addStopReplicaRequestForBrokers(List(replicaId), topic, partition, deletePartition = true,
            callbacks.stopReplicaResponseCallback)
          logStateChange()
        case ReplicaDeletionIneligible => //其前置状态只能为 ReplicaDeletionStarted
          replicaState.put(partitionAndReplica, ReplicaDeletionIneligible)
          logStateChange()
        case ReplicaDeletionSuccessful => //其前置状态只能为 ReplicaDeletionStarted
          replicaState.put(partitionAndReplica, ReplicaDeletionSuccessful)
          logStateChange()
        case NonExistentReplica => //其前置状态只能为 ReplicaDeletionSuccessful。
          // NonExistentReplica 是副本完全删除、不存在这个副本的状态
          // remove this replica from the assigned replicas list for its partition
          //在 controller 的 partitionReplicaAssignment 删除这个 Partition 对应的 replica 信息；
          val currentAssignedReplicas = controllerContext.partitionReplicaAssignment(topicAndPartition)
          controllerContext.partitionReplicaAssignment.put(topicAndPartition, currentAssignedReplicas.filterNot(_ == replicaId))
          //将这个 Topic 从缓存中删除。
          replicaState.remove(partitionAndReplica)
          logStateChange()
        case OnlineReplica =>//其前置状态只能为 NewReplica, OnlineReplica, OfflineReplica, ReplicaDeletionIneligible
          //副本正常工作时的状态，此时的 Replica 既可以作为 leader 也可以作为 follower
          replicaState(partitionAndReplica) match {
            case NewReplica => //其前置状态如果为 NewReplica
              // add this replica to the assigned replicas list for its partition
              //从 Controller 的 partitionReplicaAssignment 中获取这个 Partition 的 AR；
              val currentAssignedReplicas = controllerContext.partitionReplicaAssignment(topicAndPartition)
              //如果 Replica 不在 AR 中的话，那么就将其添加到 Partition 的 AR 中；
              if(!currentAssignedReplicas.contains(replicaId))
                controllerContext.partitionReplicaAssignment.put(topicAndPartition, currentAssignedReplicas :+ replicaId)
              logStateChange()
            case _ => //其前置状态如果为：OnlineReplica, OfflineReplica, ReplicaDeletionIneligible
              // check if the leader for this partition ever existed
              //如果该 Partition 的 LeaderIsrAndControllerEpoch 信息存在,那么就更新副本的状态,并发送相应的请求
              //否则不做任何处理；
              controllerContext.partitionLeadershipInfo.get(topicAndPartition) match {
                case Some(leaderIsrAndControllerEpoch) =>
                  brokerRequestBatch.addLeaderAndIsrRequestForBrokers(List(replicaId), topic, partition, leaderIsrAndControllerEpoch,
                    replicaAssignment)
                  replicaState.put(partitionAndReplica, OnlineReplica)
                  logStateChange()
                case None => // that means the partition was never in OnlinePartition state, this means the broker never
                             // started a log for that partition and does not have a high watermark value for this partition
              }
          }
          //最后将 Replica 的状态设置为 OnlineReplica 状态。
          replicaState.put(partitionAndReplica, OnlineReplica)
        case OfflineReplica => //其前置状态只能为 NewReplica, OnlineReplica, OfflineReplica, ReplicaDeletionIneligible
          // send stop replica command to the replica so that it stops fetching from the leader
          //发送 StopReplica 请求给该副本,先停止副本同步 （deletePartition = false）
          brokerRequestBatch.addStopReplicaRequestForBrokers(List(replicaId), topic, partition, deletePartition = false)
          // As an optimization, the controller removes dead replicas from the ISR
          val leaderAndIsrIsEmpty: Boolean =
            controllerContext.partitionLeadershipInfo.get(topicAndPartition) match {
              case Some(_) =>
                //将该 replica 从 Partition 的 isr 移除这个 replica（前提 isr 中还有其他有效副本）
                controller.removeReplicaFromIsr(topic, partition, replicaId) match {
                  case Some(updatedLeaderIsrAndControllerEpoch) =>
                    // send the shrunk ISR state change request to all the remaining alive replicas of the partition.
                    val currentAssignedReplicas = controllerContext.partitionReplicaAssignment(topicAndPartition)
                    if (!controller.topicDeletionManager.isPartitionToBeDeleted(topicAndPartition)) {
                      // 发送 LeaderAndIsr 请求给剩余的其他副本,因为 ISR 变动了
                      brokerRequestBatch.addLeaderAndIsrRequestForBrokers(currentAssignedReplicas.filterNot(_ == replicaId),
                        topic, partition, updatedLeaderIsrAndControllerEpoch, replicaAssignment)
                    }
                    //更新这个 Replica 的状态为 OfflineReplica
                    replicaState.put(partitionAndReplica, OfflineReplica)
                    logStateChange()
                    false
                  case None =>
                    true
                }
              case None =>
                true
            }
          if (leaderAndIsrIsEmpty && !controller.topicDeletionManager.isPartitionToBeDeleted(topicAndPartition))
            throw new StateChangeFailedException(
              s"Failed to change state of replica $replicaId for partition $topicAndPartition since the leader " +
                s"and isr path in zookeeper is empty")

      }
    }
    catch {
      case t: Throwable =>
        stateChangeLog.error(s"Initiated state change of replica $replicaId for partition $topicAndPartition from " +
          s"$currState to $targetState failed", t)
    }
  }

  def areAllReplicasForTopicDeleted(topic: String): Boolean = {
    val replicasForTopic = controller.controllerContext.replicasForTopic(topic)
    val replicaStatesForTopic = replicasForTopic.map(r => (r, replicaState(r))).toMap
    debug(s"Are all replicas for topic $topic deleted $replicaStatesForTopic")
    replicaStatesForTopic.forall(_._2 == ReplicaDeletionSuccessful)
  }

  def isAtLeastOneReplicaInDeletionStartedState(topic: String): Boolean = {
    val replicasForTopic = controller.controllerContext.replicasForTopic(topic)
    val replicaStatesForTopic = replicasForTopic.map(r => (r, replicaState(r))).toMap
    replicaStatesForTopic.foldLeft(false)((deletionState, r) => deletionState || r._2 == ReplicaDeletionStarted)
  }

  def replicasInState(topic: String, state: ReplicaState): Set[PartitionAndReplica] = {
    replicaState.filter(r => r._1.topic.equals(topic) && r._2 == state).keySet
  }

  def isAnyReplicaInState(topic: String, state: ReplicaState): Boolean = {
    replicaState.exists(r => r._1.topic.equals(topic) && r._2 == state)
  }

  def replicasInDeletionStates(topic: String): Set[PartitionAndReplica] = {
    val deletionStates = Set[ReplicaState](ReplicaDeletionStarted, ReplicaDeletionSuccessful, ReplicaDeletionIneligible)
    replicaState.filter(r => r._1.topic.equals(topic) && deletionStates.contains(r._2)).keySet
  }

  private def assertValidTransition(partitionAndReplica: PartitionAndReplica, targetState: ReplicaState): Unit = {
    assert(targetState.validPreviousStates.contains(replicaState(partitionAndReplica)),
      "Replica %s should be in the %s states before moving to %s state"
        .format(partitionAndReplica, targetState.validPreviousStates.mkString(","), targetState) +
        ". Instead it is in %s state".format(replicaState(partitionAndReplica)))
  }

  /**
   * Invoked on startup of the replica's state machine to set the initial state for replicas of all existing partitions
   * in zookeeper
   */
    //初始化所有副本的状态信息
    // 这里只是将 Replica 的状态信息更新副本状态机的缓存 replicaState 中，并没有真正进行状态转移的操作。
  private def initializeReplicaState() {
    for((topicPartition, assignedReplicas) <- controllerContext.partitionReplicaAssignment) {
      val topic = topicPartition.topic
      val partition = topicPartition.partition
      assignedReplicas.foreach { replicaId =>
        val partitionAndReplica = PartitionAndReplica(topic, partition, replicaId)
        //如果 Replica 所在机器是 alive 的，那么将其状态设置为 OnlineReplica
        //replicaId即brokerId
        if (controllerContext.isReplicaOnline(replicaId, topicPartition))
          replicaState.put(partitionAndReplica, OnlineReplica)
        else {
          // mark replicas on dead brokers as failed for topic deletion, if they belong to a topic to be deleted.
          // This is required during controller failover since during controller failover a broker can go down,
          // so the replicas on that broker should be moved to ReplicaDeletionIneligible to be on the safer side.
          //否则设置为 ReplicaDeletionIneligible 状态
          replicaState.put(partitionAndReplica, ReplicaDeletionIneligible)
        }
      }
    }
  }

  def partitionsAssignedToBroker(topics: Seq[String], brokerId: Int):Seq[TopicAndPartition] = {
    controllerContext.partitionReplicaAssignment.filter(_._2.contains(brokerId)).keySet.toSeq
  }

}

sealed trait ReplicaState {
  def state: Byte
  def validPreviousStates: Set[ReplicaState]
}

case object NewReplica extends ReplicaState {
  val state: Byte = 1
  val validPreviousStates: Set[ReplicaState] = Set(NonExistentReplica)
}

case object OnlineReplica extends ReplicaState {
  val state: Byte = 2
  val validPreviousStates: Set[ReplicaState] = Set(NewReplica, OnlineReplica, OfflineReplica, ReplicaDeletionIneligible)
}

case object OfflineReplica extends ReplicaState {
  val state: Byte = 3
  val validPreviousStates: Set[ReplicaState] = Set(NewReplica, OnlineReplica, OfflineReplica, ReplicaDeletionIneligible)
}

case object ReplicaDeletionStarted extends ReplicaState {
  val state: Byte = 4
  val validPreviousStates: Set[ReplicaState] = Set(OfflineReplica)
}

case object ReplicaDeletionSuccessful extends ReplicaState {
  val state: Byte = 5
  val validPreviousStates: Set[ReplicaState] = Set(ReplicaDeletionStarted)
}

case object ReplicaDeletionIneligible extends ReplicaState {
  val state: Byte = 6
  val validPreviousStates: Set[ReplicaState] = Set(ReplicaDeletionStarted)
}

case object NonExistentReplica extends ReplicaState {
  val state: Byte = 7
  val validPreviousStates: Set[ReplicaState] = Set(ReplicaDeletionSuccessful)
}
