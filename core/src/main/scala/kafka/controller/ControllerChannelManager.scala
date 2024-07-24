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

import java.net.SocketTimeoutException
import java.util.concurrent.{BlockingQueue, LinkedBlockingQueue}

import com.yammer.metrics.core.Gauge
import kafka.api._
import kafka.cluster.Broker
import kafka.common.{KafkaException, TopicAndPartition}
import kafka.metrics.KafkaMetricsGroup
import kafka.server.KafkaConfig
import kafka.utils._
import org.apache.kafka.clients._
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.network._
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.requests.UpdateMetadataRequest.EndPoint
import org.apache.kafka.common.requests._
import org.apache.kafka.common.security.JaasContext
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.apache.kafka.common.utils.{LogContext, Time}
import org.apache.kafka.common.{Node, TopicPartition}

import scala.collection.JavaConverters._
import scala.collection.mutable.HashMap
import scala.collection.{Set, mutable}


object ControllerChannelManager {
  val QueueSizeMetricName = "QueueSize"
}

//在 Kafka 中，ControllerChannelManager 是一个关键组件，负责管理控制器与其它 Kafka 代理（Broker）之间的通信。
// 控制器（Controller）是 Kafka 集群中的一个特殊角色，负责管理集群的元数据和协调集群的操作，如分区分配、副本同步等。
//
//ControllerChannelManager 的主要功能包括：
//管理通信通道：维护控制器与各个 Broker 之间的网络连接和通信通道。
//发送控制命令：向 Broker 发送控制命令，如分区重分配、副本同步等。
//接收响应：接收 Broker 发送的响应和状态更新。
//处理失败和重试：处理与 Broker 通信过程中可能出现的失败，并进行重试。
//监控和统计：监控通信状态和性能，收集统计信息。

//KafkaController 通过发送特定的请求来与 broker 通信。这些请求包括但不限于：
//LeaderAndIsrRequest：用于更新分区的 leader 和 ISR（In-Sync Replicas）。
//StopReplicaRequest：用于停止某个分区的副本。
//UpdateMetadataRequest：用于更新分区的元数据信息。
//ReassignPartitionsRequest：用于重新分配分区的副本。

//ControllerChannelManager 在初始化时，会为集群中的每个节点初始化一个 ControllerBrokerStateInfo 对象，该对象包含四个部分：
//
//NetworkClient：网络连接对象；
//Node：节点信息；
//BlockingQueue：请求队列；
//RequestSendThread：请求的发送线程。
//其具体实现如下所示：
class ControllerChannelManager(controllerContext: ControllerContext, config: KafkaConfig, time: Time, metrics: Metrics,
                               stateChangeLogger: StateChangeLogger, threadNamePrefix: Option[String] = None) extends Logging with KafkaMetricsGroup {
  import ControllerChannelManager._
  protected val brokerStateInfo = new HashMap[Int, ControllerBrokerStateInfo]
  private val brokerLock = new Object
  this.logIdent = "[Channel manager on controller " + config.brokerId + "]: "

  newGauge(
    "TotalQueueSize",
    new Gauge[Int] {
      def value: Int = brokerLock synchronized {
        brokerStateInfo.values.iterator.map(_.messageQueue.size).sum
      }
    }
  )

  //会为每个broker创建对应的RequestSendThread线程
  controllerContext.liveBrokers.foreach(addNewBroker)

  def startup() = {
    brokerLock synchronized {
      brokerStateInfo.foreach(brokerState => startRequestSendThread(brokerState._1))
    }
  }

  def shutdown() = {
    brokerLock synchronized {
      brokerStateInfo.values.foreach(removeExistingBroker)
    }
  }

  //向 broker 发送请求（并没有真正发送,只是添加到对应的 queue 中）, 请求的的发送是在 每台 Broker 对应的 RequestSendThread 中处理的。
  def sendRequest(brokerId: Int, apiKey: ApiKeys, request: AbstractRequest.Builder[_ <: AbstractRequest],
                  callback: AbstractResponse => Unit = null) {
    brokerLock synchronized {
      val stateInfoOpt = brokerStateInfo.get(brokerId)
      stateInfoOpt match {
        case Some(stateInfo) =>
          stateInfo.messageQueue.put(QueueItem(apiKey, request, callback))
        case None =>
          warn("Not sending request %s to broker %d, since it is offline.".format(request, brokerId))
      }
    }
  }

  def addBroker(broker: Broker) {
    // be careful here. Maybe the startup() API has already started the request send thread
    brokerLock synchronized {
      if(!brokerStateInfo.contains(broker.id)) {
        addNewBroker(broker)
        startRequestSendThread(broker.id)
      }
    }
  }

  def removeBroker(brokerId: Int) {
    brokerLock synchronized {
      removeExistingBroker(brokerStateInfo(brokerId))
    }
  }

  private def addNewBroker(broker: Broker) {
    val messageQueue = new LinkedBlockingQueue[QueueItem]
    debug("Controller %d trying to connect to broker %d".format(config.brokerId, broker.id))
    val brokerNode = broker.getNode(config.interBrokerListenerName)
    val logContext = new LogContext(s"[Controller id=${config.brokerId}, targetBrokerId=${brokerNode.idString}] ")
    val networkClient = {
      val channelBuilder = ChannelBuilders.clientChannelBuilder(
        config.interBrokerSecurityProtocol,
        JaasContext.Type.SERVER,
        config,
        config.interBrokerListenerName,
        config.saslMechanismInterBrokerProtocol,
        config.saslInterBrokerHandshakeRequestEnable
      )
      val selector = new Selector(
        NetworkReceive.UNLIMITED,
        Selector.NO_IDLE_TIMEOUT_MS,
        metrics,
        time,
        "controller-channel",
        Map("broker-id" -> brokerNode.idString).asJava,
        false,
        channelBuilder,
        logContext
      )
      new NetworkClient(
        selector,
        new ManualMetadataUpdater(Seq(brokerNode).asJava),
        config.brokerId.toString,
        1,
        0,
        0,
        Selectable.USE_DEFAULT_BUFFER_SIZE,
        Selectable.USE_DEFAULT_BUFFER_SIZE,
        config.requestTimeoutMs,
        time,
        false,
        new ApiVersions,
        logContext
      )
    }
    val threadName = threadNamePrefix match {
      case None => "Controller-%d-to-broker-%d-send-thread".format(config.brokerId, broker.id)
      case Some(name) => "%s:Controller-%d-to-broker-%d-send-thread".format(name, config.brokerId, broker.id)
    }

    val requestThread = new RequestSendThread(config.brokerId, controllerContext, messageQueue, networkClient,
      brokerNode, config, time, stateChangeLogger, threadName)
    requestThread.setDaemon(false)

    val queueSizeGauge = newGauge(
      QueueSizeMetricName,
      new Gauge[Int] {
        def value: Int = messageQueue.size
      },
      queueSizeTags(broker.id)
    )

    brokerStateInfo.put(broker.id, new ControllerBrokerStateInfo(networkClient, brokerNode, messageQueue,
      requestThread, queueSizeGauge))
  }

  private def queueSizeTags(brokerId: Int) = Map("broker-id" -> brokerId.toString)

  private def removeExistingBroker(brokerState: ControllerBrokerStateInfo) {
    try {
      // Shutdown the RequestSendThread before closing the NetworkClient to avoid the concurrent use of the
      // non-threadsafe classes as described in KAFKA-4959.
      // The call to shutdownLatch.await() in ShutdownableThread.shutdown() serves as a synchronization barrier that
      // hands off the NetworkClient from the RequestSendThread to the ZkEventThread.
      brokerState.requestSendThread.shutdown()
      brokerState.networkClient.close()
      brokerState.messageQueue.clear()
      removeMetric(QueueSizeMetricName, queueSizeTags(brokerState.brokerNode.id))
      brokerStateInfo.remove(brokerState.brokerNode.id)
    } catch {
      case e: Throwable => error("Error while removing broker by the controller", e)
    }
  }

  protected def startRequestSendThread(brokerId: Int) {
    val requestThread = brokerStateInfo(brokerId).requestSendThread
    if(requestThread.getState == Thread.State.NEW)
      requestThread.start()
  }
}

case class QueueItem(apiKey: ApiKeys, request: AbstractRequest.Builder[_ <: AbstractRequest],
                     callback: AbstractResponse => Unit)

class RequestSendThread(val controllerId: Int,
                        val controllerContext: ControllerContext,
                        val queue: BlockingQueue[QueueItem],
                        val networkClient: NetworkClient,
                        val brokerNode: Node,
                        val config: KafkaConfig,
                        val time: Time,
                        val stateChangeLogger: StateChangeLogger,
                        name: String)
  extends ShutdownableThread(name = name) {

  private val socketTimeoutMs = config.controllerSocketTimeoutMs

  override def doWork(): Unit = {

    def backoff(): Unit = CoreUtils.swallowTrace(Thread.sleep(100))

    val QueueItem(apiKey, requestBuilder, callback) = queue.take()
    var clientResponse: ClientResponse = null
    try {
      var isSendSuccessful = false
      while (isRunning.get() && !isSendSuccessful) {
        // if a broker goes down for a long time, then at some point the controller's zookeeper listener will trigger a
        // removeBroker which will invoke shutdown() on this thread. At that point, we will stop retrying.
        try {
          if (!brokerReady()) {
            isSendSuccessful = false
            backoff()
          }
          else {
            val clientRequest = networkClient.newClientRequest(brokerNode.idString, requestBuilder,
              time.milliseconds(), true)
            clientResponse = NetworkClientUtils.sendAndReceive(networkClient, clientRequest, time)
            isSendSuccessful = true
          }
        } catch {
          case e: Throwable => // if the send was not successful, reconnect to broker and resend the message
            warn(("Controller %d epoch %d fails to send request %s to broker %s. " +
              "Reconnecting to broker.").format(controllerId, controllerContext.epoch,
                requestBuilder.toString, brokerNode.toString), e)
            networkClient.close(brokerNode.idString)
            isSendSuccessful = false
            backoff()
        }
      }
      if (clientResponse != null) {
        val requestHeader = clientResponse.requestHeader
        val api = requestHeader.apiKey
        if (api != ApiKeys.LEADER_AND_ISR && api != ApiKeys.STOP_REPLICA && api != ApiKeys.UPDATE_METADATA)
          throw new KafkaException(s"Unexpected apiKey received: $apiKey")

        val response = clientResponse.responseBody

        stateChangeLogger.withControllerEpoch(controllerContext.epoch).trace("Received response " +
          s"${response.toString(requestHeader.apiVersion)} for a request sent to broker $brokerNode")

        if (callback != null) {
          callback(response)
        }
      }
    } catch {
      case e: Throwable =>
        error("Controller %d fails to send a request to broker %s".format(controllerId, brokerNode.toString), e)
        // If there is any socket error (eg, socket timeout), the connection is no longer usable and needs to be recreated.
        networkClient.close(brokerNode.idString)
    }
  }

  private def brokerReady(): Boolean = {
    try {
      if (!NetworkClientUtils.isReady(networkClient, brokerNode, time.milliseconds())) {
        if (!NetworkClientUtils.awaitReady(networkClient, brokerNode, time, socketTimeoutMs))
          throw new SocketTimeoutException(s"Failed to connect within $socketTimeoutMs ms")

        info("Controller %d connected to %s for sending state change requests".format(controllerId, brokerNode.toString))
      }

      true
    } catch {
      case e: Throwable =>
        warn("Controller %d's connection to broker %s was unsuccessful".format(controllerId, brokerNode.toString), e)
        networkClient.close(brokerNode.idString)
        false
    }
  }

}

class ControllerBrokerRequestBatch(controller: KafkaController, stateChangeLogger: StateChangeLogger) extends  Logging {
  val controllerContext = controller.controllerContext
  val controllerId: Int = controller.config.brokerId
  //记录每个 broker 与要发送的 LeaderAndIsr 请求集合的 map；
  val leaderAndIsrRequestMap = mutable.Map.empty[Int, mutable.Map[TopicPartition, LeaderAndIsrRequest.PartitionState]]
  //记录每个 broker 与要发送的 StopReplica 集合的 map；
  val stopReplicaRequestMap = mutable.Map.empty[Int, Seq[StopReplicaRequestInfo]]
  //记录要发送的 update-metadata 请求的 broker 集合；
  val updateMetadataRequestBrokerSet = mutable.Set.empty[Int]
  //记录 update-metadata 请求要更新的 Topic Partition 集合。
  val updateMetadataRequestPartitionInfoMap = mutable.Map.empty[TopicPartition, UpdateMetadataRequest.PartitionState]

  //创建新的请求前, 需要确保前一批请求全部发送完毕，否则抛出异常
  //这个方法的主要作用是检查上一波的 LeaderAndIsr、UpdateMetadata、StopReplica 请求是否已经发送，
  // 正常情况下，Controller 在调用 sendRequestsToBrokers() 方法之后，这些集合中的请求都会被发送，发送之后，会将相应的请求集合清空，
  // 当然在异常情况可能会导致部分集合没有被清空，导致无法 newBatch()，这种情况下，通常策略是重启 controller，因为现在 Controller 的设计还是有些复杂，
  // 在某些情况下还是可能会导致异常发生，并且有些异常还是无法恢复的。
  def newBatch() {
    // raise error if the previous batch is not empty
    if (leaderAndIsrRequestMap.nonEmpty)
      throw new IllegalStateException("Controller to broker state change requests batch is not empty while creating " +
        "a new one. Some LeaderAndIsr state changes %s might be lost ".format(leaderAndIsrRequestMap.toString()))
    if (stopReplicaRequestMap.nonEmpty)
      throw new IllegalStateException("Controller to broker state change requests batch is not empty while creating a " +
        "new one. Some StopReplica state changes %s might be lost ".format(stopReplicaRequestMap.toString()))
    if (updateMetadataRequestBrokerSet.nonEmpty)
      throw new IllegalStateException("Controller to broker state change requests batch is not empty while creating a " +
        "new one. Some UpdateMetadata state changes to brokers %s with partition info %s might be lost ".format(
          updateMetadataRequestBrokerSet.toString(), updateMetadataRequestPartitionInfoMap.toString()))
  }

  def clear() {
    leaderAndIsrRequestMap.clear()
    stopReplicaRequestMap.clear()
    updateMetadataRequestBrokerSet.clear()
    updateMetadataRequestPartitionInfoMap.clear()
  }


  //向对应的 Broker 添加 LeaderAndIsr 请求，请求会被添加到 leaderAndIsrRequestMap 集合中；
  //并通过 addUpdateMetadataRequestForBrokers() 方法向所有的 Broker 添加这个 Topic-Partition 的 UpdateMatedata 请求，
  //leader 或 isr 变动时，会向所有 broker 同步这个 Partition 的 metadata 信息，这样可以保证每台 Broker 上都有最新的 metadata 信息。
  def addLeaderAndIsrRequestForBrokers(brokerIds: Seq[Int], topic: String, partition: Int,
                                       leaderIsrAndControllerEpoch: LeaderIsrAndControllerEpoch,
                                       replicas: Seq[Int], isNew: Boolean = false) {
    val topicPartition = new TopicPartition(topic, partition)

    //将请求添加到对应的 broker 上
    brokerIds.filter(_ >= 0).foreach { brokerId =>
      val result = leaderAndIsrRequestMap.getOrElseUpdate(brokerId, mutable.Map.empty)
      val alreadyNew = result.get(topicPartition).exists(_.isNew)
      result.put(topicPartition, new LeaderAndIsrRequest.PartitionState(leaderIsrAndControllerEpoch.controllerEpoch,
        leaderIsrAndControllerEpoch.leaderAndIsr.leader,
        leaderIsrAndControllerEpoch.leaderAndIsr.leaderEpoch,
        leaderIsrAndControllerEpoch.leaderAndIsr.isr.map(Integer.valueOf).asJava,
        leaderIsrAndControllerEpoch.leaderAndIsr.zkVersion,
        replicas.map(Integer.valueOf).asJava,
        isNew || alreadyNew))
    }

    //在更新 LeaderAndIsr 信息时,主题的 metadata 相当于也进行了更新,需要发送这个 topic 的 metadata 给所有存活的 broker
    addUpdateMetadataRequestForBrokers(controllerContext.liveOrShuttingDownBrokerIds.toSeq,
                                       Set(TopicAndPartition(topic, partition)))
  }

  //向给定的 Broker 发送某个 Topic Partition 的 StopReplica 请求；
  def addStopReplicaRequestForBrokers(brokerIds: Seq[Int], topic: String, partition: Int, deletePartition: Boolean,
                                      callback: (AbstractResponse, Int) => Unit = null) {
    brokerIds.filter(b => b >= 0).foreach { brokerId =>
      stopReplicaRequestMap.getOrElseUpdate(brokerId, Seq.empty[StopReplicaRequestInfo])
      val v = stopReplicaRequestMap(brokerId)
      if(callback != null)
        stopReplicaRequestMap(brokerId) = v :+ StopReplicaRequestInfo(PartitionAndReplica(topic, partition, brokerId),
          deletePartition, (r: AbstractResponse) => callback(r, brokerId))
      else
        stopReplicaRequestMap(brokerId) = v :+ StopReplicaRequestInfo(PartitionAndReplica(topic, partition, brokerId),
          deletePartition)
    }
  }

  //向给定的 Broker 发送某一批 Partitions 的 UpdateMetadata 请求。
  //首先过滤出要发送的 Partition 列表，如果没有指定要发送 partitions 列表，那么默认就是发送全局的 metadata 信息；
  //接着将已经标记为删除的 Partition 从上面的列表中移除；
  //将要发送的 Broker 列表添加到 updateMetadataRequestBrokerSet 集合中；
  //将前面过滤的 Partition 列表对应的 metadata 信息添加到对应的 updateMetadataRequestPartitionInfoMap 集合中;
  //将当前设置为删除的所有 Partition 的 metadata 信息也添加到 updateMetadataRequestPartitionInfoMap 集合中，添加前会把其 leader 设置为-2，
  // 这样 Broker 收到这个 Partition 的 metadata 信息之后就会知道这个 Partition 是设置删除标志。
  /** Send UpdateMetadataRequest to the given brokers for the given partitions and partitions that are being deleted */
  def addUpdateMetadataRequestForBrokers(brokerIds: Seq[Int],
                                         partitions: collection.Set[TopicAndPartition] = Set.empty[TopicAndPartition]) {

    //将 Topic-Partition 添加到对应的 map 中
    def updateMetadataRequestPartitionInfo(partition: TopicAndPartition, beingDeleted: Boolean) {
      val leaderIsrAndControllerEpochOpt = controllerContext.partitionLeadershipInfo.get(partition)
      leaderIsrAndControllerEpochOpt match {
        case Some(l @ LeaderIsrAndControllerEpoch(leaderAndIsr, controllerEpoch)) =>
          val replicas = controllerContext.partitionReplicaAssignment(partition)
          val offlineReplicas = replicas.filter(!controllerContext.isReplicaOnline(_, partition))
          val leaderIsrAndControllerEpoch = if (beingDeleted) {
            val leaderDuringDelete = LeaderAndIsr.duringDelete(leaderAndIsr.isr)
            LeaderIsrAndControllerEpoch(leaderDuringDelete, controllerEpoch)
          } else {
            l
          }

          val partitionStateInfo = new UpdateMetadataRequest.PartitionState(leaderIsrAndControllerEpoch.controllerEpoch,
            leaderIsrAndControllerEpoch.leaderAndIsr.leader,
            leaderIsrAndControllerEpoch.leaderAndIsr.leaderEpoch,
            leaderIsrAndControllerEpoch.leaderAndIsr.isr.map(Integer.valueOf).asJava,
            leaderIsrAndControllerEpoch.leaderAndIsr.zkVersion,
            replicas.map(Integer.valueOf).asJava,
            offlineReplicas.map(Integer.valueOf).asJava)
          updateMetadataRequestPartitionInfoMap.put(new TopicPartition(partition.topic, partition.partition), partitionStateInfo)

        case None =>
          info("Leader not yet assigned for partition %s. Skip sending UpdateMetadataRequest.".format(partition))
      }
    }

    //过滤出要发送的 partition
    val filteredPartitions = {
      //Partitions 为空时，就过滤出所有的 topic
      val givenPartitions = if (partitions.isEmpty)
        controllerContext.partitionLeadershipInfo.keySet
      else
        partitions
      if (controller.topicDeletionManager.partitionsToBeDeleted.isEmpty)
        givenPartitions
      else
        givenPartitions -- controller.topicDeletionManager.partitionsToBeDeleted
    }

    //将 broker 列表更新到要发送的集合中
    updateMetadataRequestBrokerSet ++= brokerIds.filter(_ >= 0)
    //对于要更新 metadata 的 Partition,设置 beingDeleted 为 False
    filteredPartitions.foreach(partition => updateMetadataRequestPartitionInfo(partition, beingDeleted = false))
    //要删除的 Partition 设置 BeingDeleted 为 True
    controller.topicDeletionManager.partitionsToBeDeleted.foreach(partition => updateMetadataRequestPartitionInfo(partition, beingDeleted = true))
  }

  //发送请求给 broker（只是将对应处理后放入到对应的 queue 中）
  //此方法将三个集合中的请求发送对应 Broker 的请求队列中，这里简单作一个总结：
  //从 leaderAndIsrRequestMap 集合中构造相应的 LeaderAndIsr 请求，通过 Controller 的 sendRequest() 方法将请求添加到 Broker 对应的 MessageQueue 中，最后清空 leaderAndIsrRequestMap 集合；
  //从 updateMetadataRequestPartitionInfoMap 集合中构造相应的 UpdateMetadata 请求，，通过 Controller 的 sendRequest() 方法将请求添加到 Broker 对应的 MessageQueue 中，最后清空 updateMetadataRequestBrokerSet 和 updateMetadataRequestPartitionInfoMap 集合；
  //从 stopReplicaRequestMap 集合中构造相应的 StopReplica 请求，在构造时会根据是否设置删除标志将要涉及的 Partition 分成两类，构造对应的请求，对于要删除数据的 StopReplica 会设置相应的回调函数，然后通过 Controller 的 sendRequest() 方法将请求添加到 Broker 对应的 MessageQueue 中，最后清空 stopReplicaRequestMap 集合。
  //走到这一步，Controller 要发送的请求算是都添加到对应 Broker 的 MessageQueue 中，后台的 RequestSendThread 线程会从这个请求队列中遍历相应的请求，发送给对应的 Broker。
  def sendRequestsToBrokers(controllerEpoch: Int) {
    try {
      val stateChangeLog = stateChangeLogger.withControllerEpoch(controllerEpoch)

      val leaderAndIsrRequestVersion: Short =
        if (controller.config.interBrokerProtocolVersion >= KAFKA_1_0_IV0) 1
        else 0

      //LeaderAndIsr 请求
      leaderAndIsrRequestMap.foreach { case (broker, leaderAndIsrPartitionStates) =>
        leaderAndIsrPartitionStates.foreach { case (topicPartition, state) =>
          val typeOfRequest =
            if (broker == state.basePartitionState.leader) "become-leader"
            else "become-follower"
          stateChangeLog.trace(s"Sending $typeOfRequest LeaderAndIsr request $state to broker $broker for partition $topicPartition")
        }
        //leader id 集合
        val leaderIds = leaderAndIsrPartitionStates.map(_._2.basePartitionState.leader).toSet
        val leaders = controllerContext.liveOrShuttingDownBrokers.filter(b => leaderIds.contains(b.id)).map {
          _.getNode(controller.config.interBrokerListenerName)
        }
        //构造 LeaderAndIsr 请求,并添加到对应的 queue 中
        val leaderAndIsrRequestBuilder = new LeaderAndIsrRequest.Builder(leaderAndIsrRequestVersion, controllerId,
          controllerEpoch, leaderAndIsrPartitionStates.asJava, leaders.asJava)
        controller.sendRequest(broker, ApiKeys.LEADER_AND_ISR, leaderAndIsrRequestBuilder,
          (r: AbstractResponse) => controller.eventManager.put(controller.LeaderAndIsrResponseReceived(r, broker)))
      }
      //清空 leaderAndIsr 集合
      leaderAndIsrRequestMap.clear()

      updateMetadataRequestPartitionInfoMap.foreach { case (tp, partitionState) =>
        stateChangeLog.trace(s"Sending UpdateMetadata request $partitionState to brokers $updateMetadataRequestBrokerSet " +
          s"for partition $tp")
      }

      val partitionStates = Map.empty ++ updateMetadataRequestPartitionInfoMap
      val updateMetadataRequestVersion: Short =
        if (controller.config.interBrokerProtocolVersion >= KAFKA_1_0_IV0) 4
        else if (controller.config.interBrokerProtocolVersion >= KAFKA_0_10_2_IV0) 3
        else if (controller.config.interBrokerProtocolVersion >= KAFKA_0_10_0_IV1) 2
        else if (controller.config.interBrokerProtocolVersion >= KAFKA_0_9_0) 1
        else 0

      //构造 update-metadata 请求
      val updateMetadataRequest = {
        val liveBrokers = if (updateMetadataRequestVersion == 0) {
          // Version 0 of UpdateMetadataRequest only supports PLAINTEXT.
          controllerContext.liveOrShuttingDownBrokers.map { broker =>
            val securityProtocol = SecurityProtocol.PLAINTEXT
            val listenerName = ListenerName.forSecurityProtocol(securityProtocol)
            val node = broker.getNode(listenerName)
            val endPoints = Seq(new EndPoint(node.host, node.port, securityProtocol, listenerName))
            new UpdateMetadataRequest.Broker(broker.id, endPoints.asJava, broker.rack.orNull)
          }
        } else {
          controllerContext.liveOrShuttingDownBrokers.map { broker =>
            val endPoints = broker.endPoints.map { endPoint =>
              new UpdateMetadataRequest.EndPoint(endPoint.host, endPoint.port, endPoint.securityProtocol, endPoint.listenerName)
            }
            new UpdateMetadataRequest.Broker(broker.id, endPoints.asJava, broker.rack.orNull)
          }
        }
        new UpdateMetadataRequest.Builder(updateMetadataRequestVersion, controllerId, controllerEpoch, partitionStates.asJava,
          liveBrokers.asJava)
      }

      // 将请求添加到对应的 queue
      updateMetadataRequestBrokerSet.foreach { broker =>
        controller.sendRequest(broker, ApiKeys.UPDATE_METADATA, updateMetadataRequest, null)
      }
      updateMetadataRequestBrokerSet.clear()
      updateMetadataRequestPartitionInfoMap.clear()

      // StopReplica 请求的处理
      stopReplicaRequestMap.foreach { case (broker, replicaInfoList) =>
        val stopReplicaWithDelete = replicaInfoList.filter(_.deletePartition).map(_.replica).toSet
        val stopReplicaWithoutDelete = replicaInfoList.filterNot(_.deletePartition).map(_.replica).toSet
        debug("The stop replica request (delete = true) sent to broker %d is %s"
          .format(broker, stopReplicaWithDelete.mkString(",")))
        debug("The stop replica request (delete = false) sent to broker %d is %s"
          .format(broker, stopReplicaWithoutDelete.mkString(",")))

        val (replicasToGroup, replicasToNotGroup) = replicaInfoList.partition(r => !r.deletePartition && r.callback == null)

        // Send one StopReplicaRequest for all partitions that require neither delete nor callback. This potentially
        // changes the order in which the requests are sent for the same partitions, but that's OK.
        val stopReplicaRequest = new StopReplicaRequest.Builder(controllerId, controllerEpoch, false,
          replicasToGroup.map(r => new TopicPartition(r.replica.topic, r.replica.partition)).toSet.asJava)
        controller.sendRequest(broker, ApiKeys.STOP_REPLICA, stopReplicaRequest)

        replicasToNotGroup.foreach { r =>
          val stopReplicaRequest = new StopReplicaRequest.Builder(
              controllerId, controllerEpoch, r.deletePartition,
              Set(new TopicPartition(r.replica.topic, r.replica.partition)).asJava)
          controller.sendRequest(broker, ApiKeys.STOP_REPLICA, stopReplicaRequest, r.callback)
        }
      }
      stopReplicaRequestMap.clear()
    } catch {
      case e: Throwable =>
        if (leaderAndIsrRequestMap.nonEmpty) {
          error("Haven't been able to send leader and isr requests, current state of " +
              s"the map is $leaderAndIsrRequestMap. Exception message: $e")
        }
        if (updateMetadataRequestBrokerSet.nonEmpty) {
          error(s"Haven't been able to send metadata update requests to brokers $updateMetadataRequestBrokerSet, " +
                s"current state of the partition info is $updateMetadataRequestPartitionInfoMap. Exception message: $e")
        }
        if (stopReplicaRequestMap.nonEmpty) {
          error("Haven't been able to send stop replica requests, current state of " +
              s"the map is $stopReplicaRequestMap. Exception message: $e")
        }
        throw new IllegalStateException(e)
    }
  }
}

case class ControllerBrokerStateInfo(networkClient: NetworkClient,
                                     brokerNode: Node,
                                     messageQueue: BlockingQueue[QueueItem],
                                     requestSendThread: RequestSendThread,
                                     queueSizeGauge: Gauge[Int])

case class StopReplicaRequestInfo(replica: PartitionAndReplica, deletePartition: Boolean, callback: AbstractResponse => Unit = null)

class Callbacks private (var stopReplicaResponseCallback: (AbstractResponse, Int) => Unit)

object Callbacks {
  class CallbackBuilder {
    var stopReplicaResponseCbk: (AbstractResponse, Int) => Unit = null

    def stopReplicaCallback(cbk: (AbstractResponse, Int) => Unit): CallbackBuilder = {
      stopReplicaResponseCbk = cbk
      this
    }

    def build: Callbacks = new Callbacks(stopReplicaResponseCbk)
  }
}
