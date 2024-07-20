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

import java.util.concurrent.TimeUnit

import com.yammer.metrics.core.Gauge
import kafka.admin.{AdminUtils, PreferredReplicaLeaderElectionCommand}
import kafka.api._
import kafka.cluster.Broker
import kafka.common._
import kafka.log.LogConfig
import kafka.metrics.{KafkaMetricsGroup, KafkaTimer}
import kafka.server._
import kafka.utils.ZkUtils._
import kafka.utils._
import org.I0Itec.zkclient.exception.{ZkNoNodeException, ZkNodeExistsException}
import org.I0Itec.zkclient.{IZkChildListener, IZkDataListener, IZkStateListener}
import org.apache.kafka.common.errors.{BrokerNotAvailableException, ControllerMovedException}
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.protocol.{ApiKeys, Errors}
import org.apache.kafka.common.requests.{AbstractRequest, AbstractResponse, StopReplicaResponse, LeaderAndIsrResponse}
import org.apache.kafka.common.utils.Time
import org.apache.zookeeper.Watcher.Event.KeeperState

import scala.collection._
import scala.util.Try

class ControllerContext(val zkUtils: ZkUtils) {

  /**
   * Kafka的ControllerContext是Kafka集群中Controller组件的核心数据结构，它负责存储和管理集群的元数据信息。以下是对ControllerContext源码的详细解析：
   *
   * ControllerContext简介：
   * ControllerContext是Kafka集群中Controller组件的核心数据结构，负责存储和管理集群的元数据信息。它包含了集群中所有Broker的信息、主题信息、分区信息、副本信息等。ControllerContext通过与Zookeeper的交互来获取和更新这些元数据信息。
   *
   * ControllerContext的主要字段：
   *
   * stats：存储Controller的统计信息，如Unclean Leader选举次数等。
   * offlinePartitionCount：统计集群中所有离线或不可用状态的主题分区数量。
   * shuttingDownBrokerIds：保存所有正在关闭中的Broker的ID列表。
   * liveBrokers：当前运行中的Broker对象列表。
   * liveBrokerEpochs：运行中Broker的Epoch列表。
   * epoch：Controller当前的Epoch值。
   * epochZkVersion：Controller对应ZooKeeper节点的Epoch值。
   * allTopics：集群主题列表。
   * partitionAssignments：主题分区的副本列表。
   * partitionLeadershipInfo：主题分区的Leader/ISR副本信息。
   * partitionsBeingReassigned：正处于副本重分配过程的主题分区列表。
   * partitionStates：主题分区状态列表。
   * replicaStates：主题分区的副本状态列表。
   * replicasOnOfflineDirs：不可用磁盘路径上的副本列表。
   * topicsToBeDeleted：待删除主题列表。
   * topicsWithDeletionStarted：已开启删除的主题列表。
   * topicsIneligibleForDeletion：暂时无法执行删除的主题列表。
   * ControllerContext的初始化：
   * ControllerContext在KafkaController启动时初始化。它会从Zookeeper中读取集群的元数据信息，并在Controller的生命周期中维护这些信息。初始化过程中，ControllerContext会读取所有Broker的信息、主题信息、分区信息等，并存储在相应的字段中。
   *
   * ControllerContext的更新机制：
   * ControllerContext通过Zookeeper的Watcher机制来监听集群中的变化。当集群中的Broker上线或下线、主题创建或删除、分区分配或重新分配时，Zookeeper会触发相应的Watcher，ControllerContext会根据这些变化更新其内部的元数据信息。
   *
   * ControllerContext的作用：
   *
   * 集群元数据管理：ControllerContext负责存储和管理集群的元数据信息，包括Broker信息、主题信息、分区信息、副本信息等。
   * 协调集群操作：ControllerContext通过与Zookeeper的交互来协调集群中的各种操作，如Broker的上线和下线、主题的创建和删除、分区的分配和重新分配等。
   * 状态机管理：ControllerContext管理分区状态机和副本状态机，负责处理分区和副本的状态变化。
   * ControllerContext的监控：
   * ControllerContext提供了一些监控指标，如activeController，用于实时监控控制器的存活状态。这些监控指标在实际运维操作中非常关键，可以帮助及时发现和处理集群中的问题。
   *
   * 通过以上解析，我们可以看到ControllerContext在Kafka集群中扮演着至关重要的角色，它通过管理元数据信息和协调集群操作，确保了Kafka集群的稳定运行和高效管理。
   */
  //存储Controller的统计信息，如Unclean Leader选举次数等
  val stats = new ControllerStats

  //控制器通道管理器，负责管理与 Broker 的通信。
  var controllerChannelManager: ControllerChannelManager = null
  //保存所有正在关闭中的Broker的ID列表
  var shuttingDownBrokerIds: mutable.Set[Int] = mutable.Set.empty
  //控制器纪元（epoch），/controller_epoch节点的数据，用于标识控制器的状态版本。
  var epoch: Int = KafkaController.InitialControllerEpoch - 1
  // /controller_epoch节点的zk版本号。
  var epochZkVersion: Int = KafkaController.InitialControllerEpochZkVersion - 1
  //存储集群中所有主题的集合。
  var allTopics: Set[String] = Set.empty
  //集群的分区副本分配关系
  var partitionReplicaAssignment: mutable.Map[TopicAndPartition, Seq[Int]] = mutable.Map.empty
  //存储分区的leader副本和isr列表信息
  var partitionLeadershipInfo: mutable.Map[TopicAndPartition, LeaderIsrAndControllerEpoch] = mutable.Map.empty
  val partitionsBeingReassigned: mutable.Map[TopicAndPartition, ReassignedPartitionsContext] = new mutable.HashMap
  val replicasOnOfflineDirs: mutable.Map[Int, Set[TopicAndPartition]] = mutable.HashMap.empty

  //所有潜在的broker，包含运行中和关闭中的
  private var liveBrokersUnderlying: Set[Broker] = Set.empty
  private var liveBrokerIdsUnderlying: Set[Int] = Set.empty

  // setter
  def liveBrokers_=(brokers: Set[Broker]) {
    liveBrokersUnderlying = brokers
    liveBrokerIdsUnderlying = liveBrokersUnderlying.map(_.id)
  }

  // getter
  //当前运行中的Broker对象列表
  def liveBrokers = liveBrokersUnderlying.filter(broker => !shuttingDownBrokerIds.contains(broker.id))
  def liveBrokerIds = liveBrokerIdsUnderlying -- shuttingDownBrokerIds

  def liveOrShuttingDownBrokerIds = liveBrokerIdsUnderlying
  def liveOrShuttingDownBrokers = liveBrokersUnderlying

  def partitionsOnBroker(brokerId: Int): Set[TopicAndPartition] = {
    partitionReplicaAssignment.collect {
      case (topicAndPartition, replicas) if replicas.contains(brokerId) => topicAndPartition
    }.toSet
  }

  def isReplicaOnline(brokerId: Int, topicAndPartition: TopicAndPartition, includeShuttingDownBrokers: Boolean = false): Boolean = {
    val brokerOnline = {
      if (includeShuttingDownBrokers) liveOrShuttingDownBrokerIds.contains(brokerId)
      else liveBrokerIds.contains(brokerId)
    }
    brokerOnline && !replicasOnOfflineDirs.getOrElse(brokerId, Set.empty).contains(topicAndPartition)
  }

  def replicasOnBrokers(brokerIds: Set[Int]): Set[PartitionAndReplica] = {
    brokerIds.flatMap { brokerId =>
      partitionReplicaAssignment.collect {
        case (topicAndPartition, replicas) if replicas.contains(brokerId) =>
          PartitionAndReplica(topicAndPartition.topic, topicAndPartition.partition, brokerId)
      }
    }.toSet
  }

  def replicasForTopic(topic: String): Set[PartitionAndReplica] = {
    partitionReplicaAssignment
      .filter { case (topicAndPartition, _) => topicAndPartition.topic == topic }
      .flatMap { case (topicAndPartition, replicas) =>
        replicas.map { r =>
          PartitionAndReplica(topicAndPartition.topic, topicAndPartition.partition, r)
        }
      }.toSet
  }

  def partitionsForTopic(topic: String): collection.Set[TopicAndPartition] =
    partitionReplicaAssignment.keySet.filter(topicAndPartition => topicAndPartition.topic == topic)

  def allLiveReplicas(): Set[PartitionAndReplica] = {
    replicasOnBrokers(liveBrokerIds).filter { partitionAndReplica =>
      isReplicaOnline(partitionAndReplica.replica, TopicAndPartition(partitionAndReplica.topic, partitionAndReplica.partition))
    }
  }

  def replicasForPartition(partitions: collection.Set[TopicAndPartition]): collection.Set[PartitionAndReplica] = {
    partitions.flatMap { p =>
      val replicas = partitionReplicaAssignment(p)
      replicas.map(r => PartitionAndReplica(p.topic, p.partition, r))
    }
  }

  def removeTopic(topic: String) = {
    partitionLeadershipInfo = partitionLeadershipInfo.filter{ case (topicAndPartition, _) => topicAndPartition.topic != topic }
    partitionReplicaAssignment = partitionReplicaAssignment.filter{ case (topicAndPartition, _) => topicAndPartition.topic != topic }
    allTopics -= topic
  }

}


object KafkaController extends Logging {
  
  val InitialControllerEpoch = 1
  val InitialControllerEpochZkVersion = 1

  def parseControllerId(controllerInfoString: String): Int = {
    try {
      Json.parseFull(controllerInfoString) match {
        case Some(js) => js.asJsonObject("brokerid").to[Int]
        case None => throw new KafkaException("Failed to parse the controller info json [%s].".format(controllerInfoString))
      }
    } catch {
      case _: Throwable =>
        // It may be due to an incompatible controller register version
        warn("Failed to parse the controller info as json. "
          + "Probably this controller is still using the old format [%s] to store the broker id in zookeeper".format(controllerInfoString))
        try controllerInfoString.toInt
        catch {
          case t: Throwable => throw new KafkaException("Failed to parse the controller info: " + controllerInfoString + ". This is neither the new or the old format.", t)
        }
    }
  }
}

class KafkaController(val config: KafkaConfig, zkUtils: ZkUtils, time: Time, metrics: Metrics, threadNamePrefix: Option[String] = None) extends Logging with KafkaMetricsGroup {

  this.logIdent = s"[Controller id=${config.brokerId}] "

  private val stateChangeLogger = new StateChangeLogger(config.brokerId, inControllerContext = true, None)
  val controllerContext = new ControllerContext(zkUtils)
  val partitionStateMachine = new PartitionStateMachine(this, stateChangeLogger)
  val replicaStateMachine = new ReplicaStateMachine(this, stateChangeLogger)

  // have a separate scheduler for the controller to be able to start and stop independently of the kafka server
  // visible for testing
  private[controller] val kafkaScheduler = new KafkaScheduler(1)

  // visible for testing
  private[controller] val eventManager = new ControllerEventManager(controllerContext.stats.rateAndTimeMetrics,
    _ => updateMetrics())

  val topicDeletionManager = new TopicDeletionManager(this, eventManager)
  //Controller 原生的四种 leader 选举机制
  //
  //OfflinePartitionLeaderSelector：
  //
  //触发条件：当分区的 leader 副本掉线时触发。
  //选举逻辑：
  //如果 ISR（In-Sync Replicas）中至少有一个副本存活，那么从存活的 ISR 中选举第一个副本作为新的 leader，存活的 ISR 作为新的 ISR。
  //如果脏选举（unclean election）是禁止的，那么就抛出 NoReplicaOnlineException 异常。
  //如果允许脏选举，从存活的、所分配的副本（不在 ISR 中的副本）中选出一个副本作为新的 leader 和新的 ISR 集合。
  //如果分区分配的副本没有存活的，抛出 NoReplicaOnlineException 异常。
  //ReassignedPartitionLeaderSelector：
  //
  //触发条件：在分区的副本重新分配数据同步完成后触发。
  //选举逻辑：
  //leader 选择存活的 RAR（Reassigned Replicas）中的第一个副本，此时 RAR 都在 ISR 中。
  //新的 ISR 是所有存活的 RAR 副本列表。
  //PreferredReplicaPartitionLeaderSelector：
  //
  //触发条件：最优 leader 选举，手动触发或自动 leader 均衡调度时触发。
  //选举逻辑：
  //选择 Preferred Replica 作为 leader，如果 Preferred Replica 在 ISR 中，则直接选择。
  //如果 Preferred Replica 不在 ISR 中，需要等待其同步数据并进入 ISR 后再进行选举。
  //这种选举机制的目的是尽量保持 leader 在 Preferred Replica 上，以减少 leader 切换的频率和数据同步的开销。
  //ControlledShutdownLeaderSelector：
  //
  //触发条件：Broker 发送 ShutDown 请求主动关闭服务时触发。
  //选举逻辑：
  //在 Broker 主动关闭时，Controller 会使用这种选举机制来确保数据的一致性和完整性。
  //它会优先选择 ISR 中的副本作为新的 leader，以确保数据不丢失。
  //如果 ISR 中没有副本，可能会选择非 ISR 中的副本作为新的 leader，但这取决于配置参数 unclean.leader.election.enable。
  val offlinePartitionSelector = new OfflinePartitionLeaderSelector(controllerContext, config)
  private val reassignedPartitionLeaderSelector = new ReassignedPartitionLeaderSelector(controllerContext)
  private val preferredReplicaPartitionLeaderSelector = new PreferredReplicaPartitionLeaderSelector(controllerContext)
  private val controlledShutdownPartitionLeaderSelector = new ControlledShutdownLeaderSelector(controllerContext)
  private val brokerRequestBatch = new ControllerBrokerRequestBatch(this, stateChangeLogger)

  private val brokerChangeListener = new BrokerChangeListener(this, eventManager)
  private val topicChangeListener = new TopicChangeListener(this, eventManager)
  private val topicDeletionListener = new TopicDeletionListener(this, eventManager)
  private val partitionModificationsListeners: mutable.Map[String, PartitionModificationsListener] = mutable.Map.empty
  private val partitionReassignmentListener = new PartitionReassignmentListener(this, eventManager)
  private val preferredReplicaElectionListener = new PreferredReplicaElectionListener(this, eventManager)
  private val isrChangeNotificationListener = new IsrChangeNotificationListener(this, eventManager)
  private val logDirEventNotificationListener = new LogDirEventNotificationListener(this, eventManager)

  @volatile private var activeControllerId = -1
  //统计集群中所有离线或不可用状态的主题分区数量
  @volatile private var offlinePartitionCount = 0
  @volatile private var preferredReplicaImbalanceCount = 0
  @volatile private var globalTopicCount = 0
  @volatile private var globalPartitionCount = 0

  newGauge(
    "ActiveControllerCount",
    new Gauge[Int] {
      def value = if (isActive) 1 else 0
    }
  )

  newGauge(
    "OfflinePartitionsCount",
    new Gauge[Int] {
      def value: Int = offlinePartitionCount
    }
  )

  newGauge(
    "PreferredReplicaImbalanceCount",
    new Gauge[Int] {
      def value: Int = preferredReplicaImbalanceCount
    }
  )

  newGauge(
    "ControllerState",
    new Gauge[Byte] {
      def value: Byte = state.value
    }
  )

  newGauge(
    "GlobalTopicCount",
    new Gauge[Int] {
      def value: Int = globalTopicCount
    }
  )

  newGauge(
    "GlobalPartitionCount",
    new Gauge[Int] {
      def value: Int = globalPartitionCount
    }
  )

  def epoch: Int = controllerContext.epoch

  def state: ControllerState = eventManager.state

  def clientId: String = {
    val controllerListener = config.listeners.find(_.listenerName == config.interBrokerListenerName).getOrElse(
      throw new IllegalArgumentException(s"No listener with name ${config.interBrokerListenerName} is configured."))
    "id_%d-host_%s-port_%d".format(config.brokerId, controllerListener.host, controllerListener.port)
  }

  /**
   * On clean shutdown, the controller first determines the partitions that the
   * shutting down broker leads, and moves leadership of those partitions to another broker
   * that is in that partition's ISR.
   *
   * @param id Id of the broker to shutdown.
   * @return The number of partitions that the broker still leads.
   */
  def shutdownBroker(id: Int, controlledShutdownCallback: Try[Set[TopicAndPartition]] => Unit): Unit = {
    val controlledShutdownEvent = ControlledShutdown(id, controlledShutdownCallback)
    eventManager.put(controlledShutdownEvent)
  }


  /**
   * This callback is invoked by the zookeeper leader elector on electing the current broker as the new controller.
   * It does the following things on the become-controller state change -
   * 1. Register controller epoch changed listener
   * 2. Increments the controller epoch
   * 3. Initializes the controller's context object that holds cache objects for current topics, live brokers and
   *    leaders for all existing partitions.
   * 4. Starts the controller's channel manager
   * 5. Starts the replica state machine
   * 6. Starts the partition state machine
   * If it encounters any unexpected exception/error while becoming controller, it resigns as the current controller.
   * This ensures another controller election will be triggered and there will always be an actively serving controller
   */
  //broker成功竞选为控制器后，会调用此方法进行一些初始化操作
  def onControllerFailover() {
    info("Starting become controller state transition")
    //从zk的/controller_epoch节点中读取数据，并赋值给epoch和epochZkVersion字段
    readControllerEpochFromZookeeper()
    //将epoch加1，并更新至zk中的/controller_epoch节点，并重新赋值给epoch和epochZkVersion字段
    incrementControllerEpoch()
    LogDirUtils.deleteLogDirEvents(zkUtils)

    // before reading source of truth from zookeeper, register the listeners to get broker/topic callbacks
    //监听/admin/reassign_partitions节点的数据变化，这个节点用于存储分区副本迁移的计划和状态。
    /* 节点的json数据示例：
   {
      "version": 1,
      "partitions": [
        {
          "topic": "my-topic",
          "partition": 0,
          "replicas": [1, 2],
          "log_dirs": ["/kafka-logs-1", "/kafka-logs-2"]
        }
      ]
    }
     */
    registerPartitionReassignmentListener()
    //监控路径【/isr_change_notification】，isr 变动监听
    registerIsrChangeNotificationListener()
    //监听路径【/admin/preferred_replica_election】，最优 leader 选举
    registerPreferredReplicaElectionListener()
    //注册/brokers/topics下的主题变化监听器
    registerTopicChangeListener()
    //注册/admin/delete_topics下的主题删除监听器
    registerTopicDeletionListener()
    //注册/brokers/ids下的broker变化监听器
    registerBrokerChangeListener()
    registerLogDirEventNotificationListener()

    //初始化 Controller 的上下文信息，更新 Controller 的相关缓存信息、并启动 ControllerChannelManager 等；
    initializeControllerContext()
    val (topicsToBeDeleted, topicsIneligibleForDeletion) = fetchTopicDeletionsInProgress()
    topicDeletionManager.init(topicsToBeDeleted, topicsIneligibleForDeletion)

    // We need to send UpdateMetadataRequest after the controller context is initialized and before the state machines
    // are started. The is because brokers need to receive the list of live brokers from UpdateMetadataRequest before
    // they can process the LeaderAndIsrRequests that are generated by replicaStateMachine.startup() and
    // partitionStateMachine.startup().
    //向所有 alive 的 broker 发送 Update-Metadata 请求，broker 通过这个请求获取当前集群中 alive 的 broker 列表
    sendUpdateMetadataRequest(controllerContext.liveOrShuttingDownBrokerIds.toSeq)

    //在 KafkaController 中
    //有两个状态机：分区状态机和副本状态机；
    //一个管理器：Channel 管理器，负责管理所有的 Broker 通信；
    //相关缓存：Partition 信息、Topic 信息、broker id 信息等；
    //四种 leader 选举机制：分别是用 leader offline、broker 掉线、partition reassign、最优 leader 选举时触发；
    //启动副本状态机，初始化所有 Replica 的状态信息，如果 Replica 所在节点是 alive 的，那么状态更新为 OnlineReplica, 否则更新为 ReplicaDeletionIneligible；
    replicaStateMachine.startup()
    //启动分区状态机，初始化所有 Partition 的状态信息，如果 leader 所在 broker 是 alive 的，那么状态更新为 OnlinePartition，否则更新为 OfflinePartition
    partitionStateMachine.startup()

    // register the partition change listeners for all existing topics on failover
    //为当前所有 topic 注册一个 PartitionModificationsListener 监听器，监听所有 Topic 分区数的变化；
    controllerContext.allTopics.foreach(topic => registerPartitionModificationsListener(topic))
    info(s"Ready to serve as the new controller with epoch $epoch")
    //触发一次分区副本迁移的操作
    maybeTriggerPartitionReassignment()
    topicDeletionManager.tryTopicDeletion()
    val pendingPreferredReplicaElections = fetchPendingPreferredReplicaElections()
    //触发一次最优 leader 选举操作
    //最优 leader 选举：就是默认选择 Replica 分配中第一个 replica 作为 leader，为什么叫做最优 leader 选举呢？因为 Kafka 在给每个 Partition 分配副本时，
    // 它会保证分区的主副本会均匀分布在所有的 broker 上，这样的话只要保证第一个 replica 被选举为 leader，读写流量就会均匀分布在所有的 Broker 上，
    // 当然这是有一个前提的，那就是每个 Partition 的读写流量相差不多，但是在实际的生产环境，这是不太可能的，所以一般情况下，
    // 大集群是不建议开自动 leader 均衡的，可以通过额外的算法计算、手动去触发最优 leader 选举。
    onPreferredReplicaElection(pendingPreferredReplicaElections)
    info("Starting the controller scheduler")
    //初始化定时器
    kafkaScheduler.startup()
    //如果开启了自动 leader 均衡，启动自动 leader 均衡线程，它会根据配置的信息定时运行。
    if (config.autoLeaderRebalanceEnable) {
      scheduleAutoLeaderRebalanceTask(delay = 5, unit = TimeUnit.SECONDS)
    }
  }

  private def scheduleAutoLeaderRebalanceTask(delay: Long, unit: TimeUnit): Unit = {
    kafkaScheduler.schedule("auto-leader-rebalance-task", () => eventManager.put(AutoPreferredReplicaLeaderElection),
      delay = delay, unit = unit)
  }

  //onControllerResignation 方法的主要作用是清理控制器相关的资源和状态，确保在控制器角色变更时能够平滑过渡。
  /**
   * This callback is invoked by the zookeeper leader elector when the current broker resigns as the controller. This is
   * required to clean up internal controller data structures
   */
  def onControllerResignation() {
    debug("Resigning")
    // de-register listeners
    //取消注册的各种监听器。
    //取消 ISR 列表变更监听器的注册。
    deregisterIsrChangeNotificationListener()
    //取消分区重分配监听器的注册。
    deregisterPartitionReassignmentListener()
    //取消优先副本选举监听器的注册
    deregisterPreferredReplicaElectionListener()
    //取消日志目录变更监听器的注册
    deregisterLogDirEventNotificationListener()

    //关闭主题删除管理器。
    // reset topic deletion manager
    topicDeletionManager.reset()

    //关闭分区leader负载均衡定时器
    // shutdown leader rebalance scheduler
    kafkaScheduler.shutdown()
    offlinePartitionCount = 0
    preferredReplicaImbalanceCount = 0
    globalTopicCount = 0
    globalPartitionCount = 0

    // de-register partition ISR listener for on-going partition reassignment task
    deregisterPartitionReassignmentIsrChangeListeners()
    //关闭状态机
    // shutdown partition state machine
    partitionStateMachine.shutdown()
    deregisterTopicChangeListener()
    partitionModificationsListeners.keys.foreach(deregisterPartitionModificationsListener)
    deregisterTopicDeletionListener()
    // shutdown replica state machine
    replicaStateMachine.shutdown()
    deregisterBrokerChangeListener()

    //重置控制器上下文
    resetControllerContext()

    info("Resigned")
  }

  /**
   * Returns true if this broker is the current controller.
   */
  def isActive: Boolean = activeControllerId == config.brokerId

  /*
   * This callback is invoked by the controller's LogDirEventNotificationListener with the list of broker ids who
   * have experienced new log directory failures. In response the controller should send LeaderAndIsrRequest
   * to all these brokers to query the state of their replicas
   */
  def onBrokerLogDirFailure(brokerIds: Seq[Int]) {
    // send LeaderAndIsrRequest for all replicas on those brokers to see if they are still online.
    val replicasOnBrokers = controllerContext.replicasOnBrokers(brokerIds.toSet)
    replicaStateMachine.handleStateChanges(replicasOnBrokers, OnlineReplica)
  }

  /**
   * This callback is invoked by the replica state machine's broker change listener, with the list of newly started
   * brokers as input. It does the following -
   * 1. Sends update metadata request to all live and shutting down brokers
   * 2. Triggers the OnlinePartition state change for all new/offline partitions
   * 3. It checks whether there are reassigned replicas assigned to any newly started brokers.  If
   *    so, it performs the reassignment logic for each topic/partition.
   *
   * Note that we don't need to refresh the leader/isr cache for all topic/partitions at this point for two reasons:
   * 1. The partition state machine, when triggering online state change, will refresh leader and ISR for only those
   *    partitions currently new or offline (rather than every partition this controller is aware of)
   * 2. Even if we do refresh the cache, there is no guarantee that by the time the leader and ISR request reaches
   *    every broker that it is still valid.  Brokers check the leader epoch to determine validity of the request.
   */
  def onBrokerStartup(newBrokers: Seq[Int]) {
    info("New broker startup callback for %s".format(newBrokers.mkString(",")))
    newBrokers.foreach(controllerContext.replicasOnOfflineDirs.remove)
    val newBrokersSet = newBrokers.toSet
    // send update metadata request to all live and shutting down brokers. Old brokers will get to know of the new
    // broker via this update.
    // In cases of controlled shutdown leaders will not be elected when a new broker comes up. So at least in the
    // common controlled shutdown case, the metadata will reach the new brokers faster
    sendUpdateMetadataRequest(controllerContext.liveOrShuttingDownBrokerIds.toSeq)
    // the very first thing to do when a new broker comes up is send it the entire list of partitions that it is
    // supposed to host. Based on that the broker starts the high watermark threads for the input list of partitions
    val allReplicasOnNewBrokers = controllerContext.replicasOnBrokers(newBrokersSet)
    replicaStateMachine.handleStateChanges(allReplicasOnNewBrokers, OnlineReplica)
    // when a new broker comes up, the controller needs to trigger leader election for all new and offline partitions
    // to see if these brokers can become leaders for some/all of those
    partitionStateMachine.triggerOnlinePartitionStateChange()
    // check if reassignment of some partitions need to be restarted
    val partitionsWithReplicasOnNewBrokers = controllerContext.partitionsBeingReassigned.filter {
      case (_, reassignmentContext) => reassignmentContext.newReplicas.exists(newBrokersSet.contains(_))
    }
    partitionsWithReplicasOnNewBrokers.foreach(p => onPartitionReassignment(p._1, p._2))
    // check if topic deletion needs to be resumed. If at least one replica that belongs to the topic being deleted exists
    // on the newly restarted brokers, there is a chance that topic deletion can resume
    val replicasForTopicsToBeDeleted = allReplicasOnNewBrokers.filter(p => topicDeletionManager.isTopicQueuedUpForDeletion(p.topic))
    if(replicasForTopicsToBeDeleted.nonEmpty) {
      info(("Some replicas %s for topics scheduled for deletion %s are on the newly restarted brokers %s. " +
        "Signaling restart of topic deletion for these topics").format(replicasForTopicsToBeDeleted.mkString(","),
        topicDeletionManager.topicsToBeDeleted.mkString(","), newBrokers.mkString(",")))
      topicDeletionManager.resumeDeletionForTopics(replicasForTopicsToBeDeleted.map(_.topic))
    }
  }

  /*
   * This callback is invoked by the replica state machine's broker change listener with the list of failed brokers
   * as input. It will call onReplicaBecomeOffline(...) with the list of replicas on those failed brokers as input.
   */
  def onBrokerFailure(deadBrokers: Seq[Int]) {
    info("Broker failure callback for %s".format(deadBrokers.mkString(",")))
    deadBrokers.foreach(controllerContext.replicasOnOfflineDirs.remove)
    val deadBrokersThatWereShuttingDown =
      deadBrokers.filter(id => controllerContext.shuttingDownBrokerIds.remove(id))
    info("Removed %s from list of shutting down brokers.".format(deadBrokersThatWereShuttingDown))
    val allReplicasOnDeadBrokers = controllerContext.replicasOnBrokers(deadBrokers.toSet)
    onReplicasBecomeOffline(allReplicasOnDeadBrokers)
  }

  /**
    * This method marks the given replicas as offline. It does the following -
    * 1. Mark the given partitions as offline
    * 2. Triggers the OnlinePartition state change for all new/offline partitions
    * 3. Invokes the OfflineReplica state change on the input list of newly offline replicas
    * 4. If no partitions are affected then send UpdateMetadataRequest to live or shutting down brokers
    *
    * Note that we don't need to refresh the leader/isr cache for all topic/partitions at this point.  This is because
    * the partition state machine will refresh our cache for us when performing leader election for all new/offline
    * partitions coming online.
    */
  def onReplicasBecomeOffline(newOfflineReplicas: Set[PartitionAndReplica]): Unit = {
    val (newOfflineReplicasForDeletion, newOfflineReplicasNotForDeletion) =
      newOfflineReplicas.partition(p => topicDeletionManager.isTopicQueuedUpForDeletion(p.topic))

    val partitionsWithoutLeader = controllerContext.partitionLeadershipInfo.filter(partitionAndLeader =>
      !controllerContext.isReplicaOnline(partitionAndLeader._2.leaderAndIsr.leader, partitionAndLeader._1) &&
        !topicDeletionManager.isTopicQueuedUpForDeletion(partitionAndLeader._1.topic)).keySet

    // trigger OfflinePartition state for all partitions whose current leader is one amongst the newOfflineReplicas
    partitionStateMachine.handleStateChanges(partitionsWithoutLeader, OfflinePartition)
    // trigger OnlinePartition state changes for offline or new partitions
    partitionStateMachine.triggerOnlinePartitionStateChange()
    // trigger OfflineReplica state change for those newly offline replicas
    replicaStateMachine.handleStateChanges(newOfflineReplicasNotForDeletion, OfflineReplica)

    // fail deletion of topics that affected by the offline replicas
    if (newOfflineReplicasForDeletion.nonEmpty) {
      // it is required to mark the respective replicas in TopicDeletionFailed state since the replica cannot be
      // deleted when its log directory is offline. This will prevent the replica from being in TopicDeletionStarted state indefinitely
      // since topic deletion cannot be retried until at least one replica is in TopicDeletionStarted state
      topicDeletionManager.failReplicaDeletion(newOfflineReplicasForDeletion)
    }

    // If replica failure did not require leader re-election, inform brokers of the offline replica
    // Note that during leader re-election, brokers update their metadata
    if (partitionsWithoutLeader.isEmpty) {
      sendUpdateMetadataRequest(controllerContext.liveOrShuttingDownBrokerIds.toSeq)
    }
  }

  /**
   * This callback is invoked by the partition state machine's topic change listener with the list of new topics
   * and partitions as input. It does the following -
   * 1. Registers partition change listener. This is not required until KAFKA-347
   * 2. Invokes the new partition callback
   * 3. Send metadata request with the new topic to all brokers so they allow requests for that topic to be served
   */
  def onNewTopicCreation(topics: Set[String], newPartitions: Set[TopicAndPartition]) {
    info("New topic creation callback for %s".format(newPartitions.mkString(",")))
    // subscribe to partition changes
    topics.foreach(topic => registerPartitionModificationsListener(topic))
    onNewPartitionCreation(newPartitions)
  }

  /**
   * This callback is invoked by the topic change callback with the list of failed brokers as input.
   * It does the following -
   * 1. Move the newly created partitions to the NewPartition state
   * 2. Move the newly created partitions from NewPartition->OnlinePartition state
   */
  def onNewPartitionCreation(newPartitions: Set[TopicAndPartition]) {
    info("New partition creation callback for %s".format(newPartitions.mkString(",")))
    partitionStateMachine.handleStateChanges(newPartitions, NewPartition)
    replicaStateMachine.handleStateChanges(controllerContext.replicasForPartition(newPartitions), NewReplica)
    partitionStateMachine.handleStateChanges(newPartitions, OnlinePartition, offlinePartitionSelector)
    replicaStateMachine.handleStateChanges(controllerContext.replicasForPartition(newPartitions), OnlineReplica)
  }

  /**
   * This callback is invoked by the reassigned partitions listener. When an admin command initiates a partition
   * reassignment, it creates the /admin/reassign_partitions path that triggers the zookeeper listener.
   * Reassigning replicas for a partition goes through a few steps listed in the code.
   * RAR = Reassigned replicas
   * OAR = Original list of replicas for partition
   * AR = current assigned replicas
   *
   * 1. Update AR in ZK with OAR + RAR.
   * 2. Send LeaderAndIsr request to every replica in OAR + RAR (with AR as OAR + RAR). We do this by forcing an update
   *    of the leader epoch in zookeeper.
   * 3. Start new replicas RAR - OAR by moving replicas in RAR - OAR to NewReplica state.
   * 4. Wait until all replicas in RAR are in sync with the leader.
   * 5  Move all replicas in RAR to OnlineReplica state.
   * 6. Set AR to RAR in memory.
   * 7. If the leader is not in RAR, elect a new leader from RAR. If new leader needs to be elected from RAR, a LeaderAndIsr
   *    will be sent. If not, then leader epoch will be incremented in zookeeper and a LeaderAndIsr request will be sent.
   *    In any case, the LeaderAndIsr request will have AR = RAR. This will prevent the leader from adding any replica in
   *    RAR - OAR back in the isr.
   * 8. Move all replicas in OAR - RAR to OfflineReplica state. As part of OfflineReplica state change, we shrink the
   *    isr to remove OAR - RAR in zookeeper and send a LeaderAndIsr ONLY to the Leader to notify it of the shrunk isr.
   *    After that, we send a StopReplica (delete = false) to the replicas in OAR - RAR.
   * 9. Move all replicas in OAR - RAR to NonExistentReplica state. This will send a StopReplica (delete = true) to
   *    the replicas in OAR - RAR to physically delete the replicas on disk.
   * 10. Update AR in ZK with RAR.
   * 11. Update the /admin/reassign_partitions path in ZK to remove this partition.
   * 12. After electing leader, the replicas and isr information changes. So resend the update metadata request to every broker.
   *
   * For example, if OAR = {1, 2, 3} and RAR = {4,5,6}, the values in the assigned replica (AR) and leader/isr path in ZK
   * may go through the following transition.
   * AR                 leader/isr
   * {1,2,3}            1/{1,2,3}           (initial state)
   * {1,2,3,4,5,6}      1/{1,2,3}           (step 2)
   * {1,2,3,4,5,6}      1/{1,2,3,4,5,6}     (step 4)
   * {1,2,3,4,5,6}      4/{1,2,3,4,5,6}     (step 7)
   * {1,2,3,4,5,6}      4/{4,5,6}           (step 8)
   * {4,5,6}            4/{4,5,6}           (step 10)
   *
   * Note that we have to update AR in ZK with RAR last since it's the only place where we store OAR persistently.
   * This way, if the controller crashes before that step, we can still recover.
   */
  def onPartitionReassignment(topicAndPartition: TopicAndPartition, reassignedPartitionContext: ReassignedPartitionsContext) {
    val reassignedReplicas = reassignedPartitionContext.newReplicas
    if (!areReplicasInIsr(topicAndPartition.topic, topicAndPartition.partition, reassignedReplicas)) {
      info("New replicas %s for partition %s being ".format(reassignedReplicas.mkString(","), topicAndPartition) +
        "reassigned not yet caught up with the leader")
      val newReplicasNotInOldReplicaList = reassignedReplicas.toSet -- controllerContext.partitionReplicaAssignment(topicAndPartition).toSet
      val newAndOldReplicas = (reassignedPartitionContext.newReplicas ++ controllerContext.partitionReplicaAssignment(topicAndPartition)).toSet
      //1. Update AR in ZK with OAR + RAR.
      updateAssignedReplicasForPartition(topicAndPartition, newAndOldReplicas.toSeq)
      //2. Send LeaderAndIsr request to every replica in OAR + RAR (with AR as OAR + RAR).
      updateLeaderEpochAndSendRequest(topicAndPartition, controllerContext.partitionReplicaAssignment(topicAndPartition),
        newAndOldReplicas.toSeq)
      //3. replicas in RAR - OAR -> NewReplica
      startNewReplicasForReassignedPartition(topicAndPartition, reassignedPartitionContext, newReplicasNotInOldReplicaList)
      info("Waiting for new replicas %s for partition %s being ".format(reassignedReplicas.mkString(","), topicAndPartition) +
        "reassigned to catch up with the leader")
    } else {
      //4. Wait until all replicas in RAR are in sync with the leader.
      val oldReplicas = controllerContext.partitionReplicaAssignment(topicAndPartition).toSet -- reassignedReplicas.toSet
      //5. replicas in RAR -> OnlineReplica
      reassignedReplicas.foreach { replica =>
        replicaStateMachine.handleStateChanges(Set(PartitionAndReplica(topicAndPartition.topic, topicAndPartition.partition,
          replica)), OnlineReplica)
      }
      //6. Set AR to RAR in memory.
      //7. Send LeaderAndIsr request with a potential new leader (if current leader not in RAR) and
      //   a new AR (using RAR) and same isr to every broker in RAR
      moveReassignedPartitionLeaderIfRequired(topicAndPartition, reassignedPartitionContext)
      //8. replicas in OAR - RAR -> Offline (force those replicas out of isr)
      //9. replicas in OAR - RAR -> NonExistentReplica (force those replicas to be deleted)
      stopOldReplicasOfReassignedPartition(topicAndPartition, reassignedPartitionContext, oldReplicas)
      //10. Update AR in ZK with RAR.
      updateAssignedReplicasForPartition(topicAndPartition, reassignedReplicas)
      //11. Update the /admin/reassign_partitions path in ZK to remove this partition.
      removePartitionFromReassignedPartitions(topicAndPartition)
      info("Removed partition %s from the list of reassigned partitions in zookeeper".format(topicAndPartition))
      controllerContext.partitionsBeingReassigned.remove(topicAndPartition)
      //12. After electing leader, the replicas and isr information changes, so resend the update metadata request to every broker
      sendUpdateMetadataRequest(controllerContext.liveOrShuttingDownBrokerIds.toSeq, Set(topicAndPartition))
      // signal delete topic thread if reassignment for some partitions belonging to topics being deleted just completed
      topicDeletionManager.resumeDeletionForTopics(Set(topicAndPartition.topic))
    }
  }

  private def watchIsrChangesForReassignedPartition(topic: String,
                                                    partition: Int,
                                                    reassignedPartitionContext: ReassignedPartitionsContext) {
    val reassignedReplicas = reassignedPartitionContext.newReplicas
    val isrChangeListener = new PartitionReassignmentIsrChangeListener(this, eventManager, topic, partition,
      reassignedReplicas.toSet)
    reassignedPartitionContext.isrChangeListener = isrChangeListener
    // register listener on the leader and isr path to wait until they catch up with the current leader
    zkUtils.subscribeDataChanges(getTopicPartitionLeaderAndIsrPath(topic, partition), isrChangeListener)
  }

  def initiateReassignReplicasForTopicPartition(topicAndPartition: TopicAndPartition,
                                                reassignedPartitionContext: ReassignedPartitionsContext) {
    val newReplicas = reassignedPartitionContext.newReplicas
    val topic = topicAndPartition.topic
    val partition = topicAndPartition.partition
    try {
      val assignedReplicasOpt = controllerContext.partitionReplicaAssignment.get(topicAndPartition)
      assignedReplicasOpt match {
        case Some(assignedReplicas) =>
          if (assignedReplicas == newReplicas) {
            throw new KafkaException("Partition %s to be reassigned is already assigned to replicas".format(topicAndPartition) +
              " %s. Ignoring request for partition reassignment".format(newReplicas.mkString(",")))
          } else {
            info("Handling reassignment of partition %s to new replicas %s".format(topicAndPartition, newReplicas.mkString(",")))
            // first register ISR change listener
            watchIsrChangesForReassignedPartition(topic, partition, reassignedPartitionContext)
            controllerContext.partitionsBeingReassigned.put(topicAndPartition, reassignedPartitionContext)
            // mark topic ineligible for deletion for the partitions being reassigned
            topicDeletionManager.markTopicIneligibleForDeletion(Set(topic))
            onPartitionReassignment(topicAndPartition, reassignedPartitionContext)
          }
        case None => throw new KafkaException("Attempt to reassign partition %s that doesn't exist"
          .format(topicAndPartition))
      }
    } catch {
      case e: Throwable => error("Error completing reassignment of partition %s".format(topicAndPartition), e)
      // remove the partition from the admin path to unblock the admin client
      removePartitionFromReassignedPartitions(topicAndPartition)
    }
  }

  def onPreferredReplicaElection(partitions: Set[TopicAndPartition], isTriggeredByAutoRebalance: Boolean = false) {
    info("Starting preferred replica leader election for partitions %s".format(partitions.mkString(",")))
    try {
      partitionStateMachine.handleStateChanges(partitions, OnlinePartition, preferredReplicaPartitionLeaderSelector)
    } catch {
      case e: Throwable => error("Error completing preferred replica leader election for partitions %s".format(partitions.mkString(",")), e)
    } finally {
      removePartitionsFromPreferredReplicaElection(partitions, isTriggeredByAutoRebalance)
    }
  }

  /**
   * Invoked when the controller module of a Kafka server is started up. This does not assume that the current broker
   * is the controller. It merely registers the session expiration listener and starts the controller leader
   * elector
   */
  def startup() = {
    //将Startup事件放入事件队列
    eventManager.put(Startup)
    //启动事件队列处理线程，会处理Startup事件
    eventManager.start()
  }

  /**
   * Invoked when the controller module of a Kafka server is shutting down. If the broker was the current controller,
   * it shuts down the partition and replica state machines. If not, those are a no-op. In addition to that, it also
   * shuts down the controller channel manager, if one exists (i.e. if it was the current controller)
   */
  def shutdown() = {
    eventManager.close()
    onControllerResignation()
  }

  def sendRequest(brokerId: Int, apiKey: ApiKeys, request: AbstractRequest.Builder[_ <: AbstractRequest],
                  callback: AbstractResponse => Unit = null) = {
    controllerContext.controllerChannelManager.sendRequest(brokerId, apiKey, request, callback)
  }

  def incrementControllerEpoch() = {
    try {
      val newControllerEpoch = controllerContext.epoch + 1
      val (updateSucceeded, newVersion) = zkUtils.conditionalUpdatePersistentPathIfExists(
        ZkUtils.ControllerEpochPath, newControllerEpoch.toString, controllerContext.epochZkVersion)
      if(!updateSucceeded)
        throw new ControllerMovedException("Controller moved to another broker. Aborting controller startup procedure")
      else {
        controllerContext.epochZkVersion = newVersion
        controllerContext.epoch = newControllerEpoch
      }
    } catch {
      case _: ZkNoNodeException =>
        // if path doesn't exist, this is the first controller whose epoch should be 1
        // the following call can still fail if another controller gets elected between checking if the path exists and
        // trying to create the controller epoch path
        try {
          zkUtils.createPersistentPath(ZkUtils.ControllerEpochPath, KafkaController.InitialControllerEpoch.toString)
          controllerContext.epoch = KafkaController.InitialControllerEpoch
          controllerContext.epochZkVersion = KafkaController.InitialControllerEpochZkVersion
        } catch {
          case _: ZkNodeExistsException => throw new ControllerMovedException("Controller moved to another broker. " +
            "Aborting controller startup procedure")
          case oe: Throwable => error("Error while incrementing controller epoch", oe)
        }
      case oe: Throwable => error("Error while incrementing controller epoch", oe)

    }
    info(s"Incremented epoch to ${controllerContext.epoch}")
  }

  private def registerSessionExpirationListener() = {
    zkUtils.subscribeStateChanges(new SessionExpirationListener(this, eventManager))
  }

  private def registerControllerChangeListener() = {
    zkUtils.subscribeDataChanges(ZkUtils.ControllerPath, new ControllerChangeListener(this, eventManager))
  }

  //用于初始化控制器的上下文环境。
  private def initializeControllerContext() {
    // update controller cache with delete topic information
    //从 zk 获取所有 alive broker 列表，记录到 liveBrokers；
    controllerContext.liveBrokers = zkUtils.getAllBrokersInCluster().toSet
    //从 zk 获取所有的 topic 列表，记录到 allTopic 中；
    controllerContext.allTopics = zkUtils.getAllTopics().toSet
    //从 zk 获取所有 Partition 的 replica 信息，更新到 partitionReplicaAssignment 中；
    controllerContext.partitionReplicaAssignment = zkUtils.getReplicaAssignmentForTopics(controllerContext.allTopics.toSeq)
    //下面两个都是新创建的空集合
    controllerContext.partitionLeadershipInfo = new mutable.HashMap[TopicAndPartition, LeaderIsrAndControllerEpoch]
    controllerContext.shuttingDownBrokerIds = mutable.Set.empty[Int]
    // update the leader and isr cache for all existing partitions from Zookeeper
    //从 zk 获取所有 Partition 的 LeaderAndIsr 信息，更新到 partitionLeadershipInfo 中；
    updateLeaderAndIsrCache()
    // start the channel manager
    //会调用controllerChannelManager.startup(), 启动 Controller 的 Channel Manager；
    startChannelManager()
    //初始化需要进行副本迁移的 Partition 列表，记录到 partitionsBeingReassigned 中；
    initializePartitionReassignment()
    info("Currently active brokers in the cluster: %s".format(controllerContext.liveBrokerIds))
    info("Currently shutting brokers in the cluster: %s".format(controllerContext.shuttingDownBrokerIds))
    info("Current list of topics in the cluster: %s".format(controllerContext.allTopics))
  }

  private def fetchPendingPreferredReplicaElections(): Set[TopicAndPartition] = {
    val partitionsUndergoingPreferredReplicaElection = zkUtils.getPartitionsUndergoingPreferredReplicaElection()
    // check if they are already completed or topic was deleted
    val partitionsThatCompletedPreferredReplicaElection = partitionsUndergoingPreferredReplicaElection.filter { partition =>
      val replicasOpt = controllerContext.partitionReplicaAssignment.get(partition)
      val topicDeleted = replicasOpt.isEmpty
      val successful =
        if(!topicDeleted) controllerContext.partitionLeadershipInfo(partition).leaderAndIsr.leader == replicasOpt.get.head else false
      successful || topicDeleted
    }
    val pendingPreferredReplicaElectionsIgnoringTopicDeletion = partitionsUndergoingPreferredReplicaElection -- partitionsThatCompletedPreferredReplicaElection
    val pendingPreferredReplicaElectionsSkippedFromTopicDeletion = pendingPreferredReplicaElectionsIgnoringTopicDeletion.filter(partition => topicDeletionManager.isTopicQueuedUpForDeletion(partition.topic))
    val pendingPreferredReplicaElections = pendingPreferredReplicaElectionsIgnoringTopicDeletion -- pendingPreferredReplicaElectionsSkippedFromTopicDeletion
    info("Partitions undergoing preferred replica election: %s".format(partitionsUndergoingPreferredReplicaElection.mkString(",")))
    info("Partitions that completed preferred replica election: %s".format(partitionsThatCompletedPreferredReplicaElection.mkString(",")))
    info("Skipping preferred replica election for partitions due to topic deletion: %s".format(pendingPreferredReplicaElectionsSkippedFromTopicDeletion.mkString(",")))
    info("Resuming preferred replica election for partitions: %s".format(pendingPreferredReplicaElections.mkString(",")))
    pendingPreferredReplicaElections
  }

  private def resetControllerContext(): Unit = {
    if (controllerContext.controllerChannelManager != null) {
      controllerContext.controllerChannelManager.shutdown()
      controllerContext.controllerChannelManager = null
    }
    controllerContext.shuttingDownBrokerIds.clear()
    controllerContext.epoch = 0
    controllerContext.epochZkVersion = 0
    controllerContext.allTopics = Set.empty
    controllerContext.partitionReplicaAssignment.clear()
    controllerContext.partitionLeadershipInfo.clear()
    controllerContext.partitionsBeingReassigned.clear()
    controllerContext.liveBrokers = Set.empty
  }

  private def initializePartitionReassignment() {
    // read the partitions being reassigned from zookeeper path /admin/reassign_partitions
    val partitionsBeingReassigned = zkUtils.getPartitionsBeingReassigned()
    // check if they are already completed or topic was deleted
    val reassignedPartitions = partitionsBeingReassigned.filter { partition =>
      val replicasOpt = controllerContext.partitionReplicaAssignment.get(partition._1)
      val topicDeleted = replicasOpt.isEmpty
      val successful = if (!topicDeleted) replicasOpt.get == partition._2.newReplicas else false
      topicDeleted || successful
    }.keys
    reassignedPartitions.foreach(p => removePartitionFromReassignedPartitions(p))
    val partitionsToReassign = mutable.Map[TopicAndPartition, ReassignedPartitionsContext]()
    partitionsToReassign ++= partitionsBeingReassigned
    partitionsToReassign --= reassignedPartitions
    controllerContext.partitionsBeingReassigned ++= partitionsToReassign
    info(s"Partitions being reassigned: $partitionsBeingReassigned")
    info(s"Partitions already reassigned: $reassignedPartitions")
    info(s"Resuming reassignment of partitions: $partitionsToReassign")
  }

  private def fetchTopicDeletionsInProgress(): (Set[String], Set[String]) = {
    val topicsToBeDeleted = zkUtils.getChildrenParentMayNotExist(ZkUtils.DeleteTopicsPath).toSet
    val topicsWithOfflineReplicas = controllerContext.partitionReplicaAssignment.filter { case (partition, replicas) =>
      replicas.exists(r => !controllerContext.isReplicaOnline(r, partition))
    }.keySet.map(_.topic)
    val topicsForWhichPartitionReassignmentIsInProgress = controllerContext.partitionsBeingReassigned.keySet.map(_.topic)
    val topicsIneligibleForDeletion = topicsWithOfflineReplicas | topicsForWhichPartitionReassignmentIsInProgress
    info("List of topics to be deleted: %s".format(topicsToBeDeleted.mkString(",")))
    info("List of topics ineligible for deletion: %s".format(topicsIneligibleForDeletion.mkString(",")))
    (topicsToBeDeleted, topicsIneligibleForDeletion)
  }

  private def maybeTriggerPartitionReassignment() {
    controllerContext.partitionsBeingReassigned.foreach { topicPartitionToReassign =>
      initiateReassignReplicasForTopicPartition(topicPartitionToReassign._1, topicPartitionToReassign._2)
    }
  }

  private def startChannelManager() {
    controllerContext.controllerChannelManager = new ControllerChannelManager(controllerContext, config, time, metrics,
      stateChangeLogger, threadNamePrefix)
    controllerContext.controllerChannelManager.startup()
  }

  def updateLeaderAndIsrCache(topicAndPartitions: Set[TopicAndPartition] = controllerContext.partitionReplicaAssignment.keySet) {
    val leaderAndIsrInfo = zkUtils.getPartitionLeaderAndIsrForTopics(topicAndPartitions)
    for ((topicPartition, leaderIsrAndControllerEpoch) <- leaderAndIsrInfo)
      controllerContext.partitionLeadershipInfo.put(topicPartition, leaderIsrAndControllerEpoch)
  }

  private def areReplicasInIsr(topic: String, partition: Int, replicas: Seq[Int]): Boolean = {
    zkUtils.getLeaderAndIsrForPartition(topic, partition).exists { leaderAndIsr =>
      replicas.forall(leaderAndIsr.isr.contains)
    }
  }

  private def moveReassignedPartitionLeaderIfRequired(topicAndPartition: TopicAndPartition,
                                                      reassignedPartitionContext: ReassignedPartitionsContext) {
    val reassignedReplicas = reassignedPartitionContext.newReplicas
    val currentLeader = controllerContext.partitionLeadershipInfo(topicAndPartition).leaderAndIsr.leader
    // change the assigned replica list to just the reassigned replicas in the cache so it gets sent out on the LeaderAndIsr
    // request to the current or new leader. This will prevent it from adding the old replicas to the ISR
    val oldAndNewReplicas = controllerContext.partitionReplicaAssignment(topicAndPartition)
    controllerContext.partitionReplicaAssignment.put(topicAndPartition, reassignedReplicas)
    if(!reassignedPartitionContext.newReplicas.contains(currentLeader)) {
      info("Leader %s for partition %s being reassigned, ".format(currentLeader, topicAndPartition) +
        "is not in the new list of replicas %s. Re-electing leader".format(reassignedReplicas.mkString(",")))
      // move the leader to one of the alive and caught up new replicas
      partitionStateMachine.handleStateChanges(Set(topicAndPartition), OnlinePartition, reassignedPartitionLeaderSelector)
    } else {
      // check if the leader is alive or not
      if (controllerContext.isReplicaOnline(currentLeader, topicAndPartition)) {
        info("Leader %s for partition %s being reassigned, ".format(currentLeader, topicAndPartition) +
          "is already in the new list of replicas %s and is alive".format(reassignedReplicas.mkString(",")))
        // shrink replication factor and update the leader epoch in zookeeper to use on the next LeaderAndIsrRequest
        updateLeaderEpochAndSendRequest(topicAndPartition, oldAndNewReplicas, reassignedReplicas)
      } else {
        info("Leader %s for partition %s being reassigned, ".format(currentLeader, topicAndPartition) +
          "is already in the new list of replicas %s but is dead".format(reassignedReplicas.mkString(",")))
        partitionStateMachine.handleStateChanges(Set(topicAndPartition), OnlinePartition, reassignedPartitionLeaderSelector)
      }
    }
  }

  private def stopOldReplicasOfReassignedPartition(topicAndPartition: TopicAndPartition,
                                                   reassignedPartitionContext: ReassignedPartitionsContext,
                                                   oldReplicas: Set[Int]) {
    val topic = topicAndPartition.topic
    val partition = topicAndPartition.partition
    // first move the replica to offline state (the controller removes it from the ISR)
    val replicasToBeDeleted = oldReplicas.map(r => PartitionAndReplica(topic, partition, r))
    replicaStateMachine.handleStateChanges(replicasToBeDeleted, OfflineReplica)
    // send stop replica command to the old replicas
    replicaStateMachine.handleStateChanges(replicasToBeDeleted, ReplicaDeletionStarted)
    // TODO: Eventually partition reassignment could use a callback that does retries if deletion failed
    replicaStateMachine.handleStateChanges(replicasToBeDeleted, ReplicaDeletionSuccessful)
    replicaStateMachine.handleStateChanges(replicasToBeDeleted, NonExistentReplica)
  }

  private def updateAssignedReplicasForPartition(topicAndPartition: TopicAndPartition,
                                                 replicas: Seq[Int]) {
    val partitionsAndReplicasForThisTopic = controllerContext.partitionReplicaAssignment.filter(_._1.topic.equals(topicAndPartition.topic))
    partitionsAndReplicasForThisTopic.put(topicAndPartition, replicas)
    updateAssignedReplicasForPartition(topicAndPartition, partitionsAndReplicasForThisTopic)
    info("Updated assigned replicas for partition %s being reassigned to %s ".format(topicAndPartition, replicas.mkString(",")))
    // update the assigned replica list after a successful zookeeper write
    controllerContext.partitionReplicaAssignment.put(topicAndPartition, replicas)
  }

  private def startNewReplicasForReassignedPartition(topicAndPartition: TopicAndPartition,
                                                     reassignedPartitionContext: ReassignedPartitionsContext,
                                                     newReplicas: Set[Int]) {
    // send the start replica request to the brokers in the reassigned replicas list that are not in the assigned
    // replicas list
    newReplicas.foreach { replica =>
      replicaStateMachine.handleStateChanges(Set(new PartitionAndReplica(topicAndPartition.topic, topicAndPartition.partition, replica)), NewReplica)
    }
  }

  private def updateLeaderEpochAndSendRequest(topicAndPartition: TopicAndPartition, replicasToReceiveRequest: Seq[Int], newAssignedReplicas: Seq[Int]) {
    val stateChangeLog = stateChangeLogger.withControllerEpoch(controllerContext.epoch)
    updateLeaderEpoch(topicAndPartition.topic, topicAndPartition.partition) match {
      case Some(updatedLeaderIsrAndControllerEpoch) =>
        try {
          brokerRequestBatch.newBatch()
          brokerRequestBatch.addLeaderAndIsrRequestForBrokers(replicasToReceiveRequest, topicAndPartition.topic,
            topicAndPartition.partition, updatedLeaderIsrAndControllerEpoch, newAssignedReplicas)
          brokerRequestBatch.sendRequestsToBrokers(controllerContext.epoch)
        } catch {
          case e: IllegalStateException =>
            handleIllegalState(e)
        }
        stateChangeLog.trace(s"Sent LeaderAndIsr request $updatedLeaderIsrAndControllerEpoch with new assigned replica " +
          s"list ${newAssignedReplicas.mkString(",")} to leader ${updatedLeaderIsrAndControllerEpoch.leaderAndIsr.leader} " +
          s"for partition being reassigned $topicAndPartition")
      case None => // fail the reassignment
        stateChangeLog.error("Failed to send LeaderAndIsr request with new assigned replica list " +
          s"${newAssignedReplicas.mkString( ",")} to leader for partition being reassigned $topicAndPartition")
    }
  }

  private def registerBrokerChangeListener() = {
    zkUtils.subscribeChildChanges(ZkUtils.BrokerIdsPath, brokerChangeListener)
  }

  private def deregisterBrokerChangeListener() = {
    zkUtils.unsubscribeChildChanges(ZkUtils.BrokerIdsPath, brokerChangeListener)
  }

  private def registerTopicChangeListener() = {
    zkUtils.subscribeChildChanges(BrokerTopicsPath, topicChangeListener)
  }

  private def deregisterTopicChangeListener() = {
    zkUtils.unsubscribeChildChanges(BrokerTopicsPath, topicChangeListener)
  }

  def registerPartitionModificationsListener(topic: String) = {
    partitionModificationsListeners.put(topic, new PartitionModificationsListener(this, eventManager, topic))
    zkUtils.subscribeDataChanges(getTopicPath(topic), partitionModificationsListeners(topic))
  }

  def deregisterPartitionModificationsListener(topic: String) = {
    zkUtils.unsubscribeDataChanges(getTopicPath(topic), partitionModificationsListeners(topic))
    partitionModificationsListeners.remove(topic)
  }

  private def registerTopicDeletionListener() = {
    zkUtils.subscribeChildChanges(DeleteTopicsPath, topicDeletionListener)
  }

  private def deregisterTopicDeletionListener() = {
    zkUtils.unsubscribeChildChanges(DeleteTopicsPath, topicDeletionListener)
  }

  private def registerPartitionReassignmentListener() = {
    zkUtils.subscribeDataChanges(ZkUtils.ReassignPartitionsPath, partitionReassignmentListener)
  }

  private def deregisterPartitionReassignmentListener() = {
    zkUtils.unsubscribeDataChanges(ZkUtils.ReassignPartitionsPath, partitionReassignmentListener)
  }

  private def registerIsrChangeNotificationListener() = {
    debug("Registering IsrChangeNotificationListener")
    zkUtils.subscribeChildChanges(ZkUtils.IsrChangeNotificationPath, isrChangeNotificationListener)
  }

  private def deregisterIsrChangeNotificationListener() = {
    debug("De-registering IsrChangeNotificationListener")
    zkUtils.unsubscribeChildChanges(ZkUtils.IsrChangeNotificationPath, isrChangeNotificationListener)
  }

  private def registerPreferredReplicaElectionListener() {
    zkUtils.subscribeDataChanges(ZkUtils.PreferredReplicaLeaderElectionPath, preferredReplicaElectionListener)
  }

  private def deregisterPreferredReplicaElectionListener() {
    zkUtils.unsubscribeDataChanges(ZkUtils.PreferredReplicaLeaderElectionPath, preferredReplicaElectionListener)
  }

  private def deregisterPartitionReassignmentIsrChangeListeners() {
    controllerContext.partitionsBeingReassigned.foreach {
      case (topicAndPartition, reassignedPartitionsContext) =>
        val zkPartitionPath = getTopicPartitionLeaderAndIsrPath(topicAndPartition.topic, topicAndPartition.partition)
        zkUtils.unsubscribeDataChanges(zkPartitionPath, reassignedPartitionsContext.isrChangeListener)
    }
  }

  private def registerLogDirEventNotificationListener() = {
    debug("Registering logDirEventNotificationListener")
    zkUtils.subscribeChildChanges(ZkUtils.LogDirEventNotificationPath, logDirEventNotificationListener)
  }

  private def deregisterLogDirEventNotificationListener() = {
    debug("De-registering logDirEventNotificationListener")
    zkUtils.unsubscribeChildChanges(ZkUtils.LogDirEventNotificationPath, logDirEventNotificationListener)
  }

  private def readControllerEpochFromZookeeper() {
    // initialize the controller epoch and zk version by reading from zookeeper
    if(controllerContext.zkUtils.pathExists(ZkUtils.ControllerEpochPath)) {
      val epochData = controllerContext.zkUtils.readData(ZkUtils.ControllerEpochPath)
      controllerContext.epoch = epochData._1.toInt
      controllerContext.epochZkVersion = epochData._2.getVersion
      info("Initialized controller epoch to %d and zk version %d".format(controllerContext.epoch, controllerContext.epochZkVersion))
    }
  }

  def removePartitionFromReassignedPartitions(topicAndPartition: TopicAndPartition) {
    if(controllerContext.partitionsBeingReassigned.get(topicAndPartition).isDefined) {
      // stop watching the ISR changes for this partition
      zkUtils.unsubscribeDataChanges(getTopicPartitionLeaderAndIsrPath(topicAndPartition.topic, topicAndPartition.partition),
        controllerContext.partitionsBeingReassigned(topicAndPartition).isrChangeListener)
    }
    // read the current list of reassigned partitions from zookeeper
    val partitionsBeingReassigned = zkUtils.getPartitionsBeingReassigned()
    // remove this partition from that list
    val updatedPartitionsBeingReassigned = partitionsBeingReassigned - topicAndPartition
    // write the new list to zookeeper
    zkUtils.updatePartitionReassignmentData(updatedPartitionsBeingReassigned.mapValues(_.newReplicas))
    // update the cache. NO-OP if the partition's reassignment was never started
    controllerContext.partitionsBeingReassigned.remove(topicAndPartition)
  }

  def updateAssignedReplicasForPartition(topicAndPartition: TopicAndPartition,
                                         newReplicaAssignmentForTopic: Map[TopicAndPartition, Seq[Int]]) {
    try {
      val zkPath = getTopicPath(topicAndPartition.topic)
      val jsonPartitionMap = zkUtils.replicaAssignmentZkData(newReplicaAssignmentForTopic.map(e => e._1.partition.toString -> e._2))
      zkUtils.updatePersistentPath(zkPath, jsonPartitionMap)
      debug("Updated path %s with %s for replica assignment".format(zkPath, jsonPartitionMap))
    } catch {
      case _: ZkNoNodeException => throw new IllegalStateException("Topic %s doesn't exist".format(topicAndPartition.topic))
      case e2: Throwable => throw new KafkaException(e2.toString)
    }
  }

  def removePartitionsFromPreferredReplicaElection(partitionsToBeRemoved: Set[TopicAndPartition],
                                                   isTriggeredByAutoRebalance : Boolean) {
    for(partition <- partitionsToBeRemoved) {
      // check the status
      val currentLeader = controllerContext.partitionLeadershipInfo(partition).leaderAndIsr.leader
      val preferredReplica = controllerContext.partitionReplicaAssignment(partition).head
      if(currentLeader == preferredReplica) {
        info("Partition %s completed preferred replica leader election. New leader is %d".format(partition, preferredReplica))
      } else {
        warn("Partition %s failed to complete preferred replica leader election. Leader is %d".format(partition, currentLeader))
      }
    }
    if (!isTriggeredByAutoRebalance)
      zkUtils.deletePath(ZkUtils.PreferredReplicaLeaderElectionPath)
  }

  /**
   * Send the leader information for selected partitions to selected brokers so that they can correctly respond to
   * metadata requests
   *
   * @param brokers The brokers that the update metadata request should be sent to
   */
  def sendUpdateMetadataRequest(brokers: Seq[Int], partitions: Set[TopicAndPartition] = Set.empty[TopicAndPartition]) {
    try {
      brokerRequestBatch.newBatch()
      brokerRequestBatch.addUpdateMetadataRequestForBrokers(brokers, partitions)
      brokerRequestBatch.sendRequestsToBrokers(epoch)
    } catch {
      case e: IllegalStateException =>
        handleIllegalState(e)
    }
  }

  /**
   * Removes a given partition replica from the ISR; if it is not the current
   * leader and there are sufficient remaining replicas in ISR.
   *
   * @param topic topic
   * @param partition partition
   * @param replicaId replica Id
   * @return the new leaderAndIsr (with the replica removed if it was present),
   *         or None if leaderAndIsr is empty.
   */
  def removeReplicaFromIsr(topic: String, partition: Int, replicaId: Int): Option[LeaderIsrAndControllerEpoch] = {
    val topicAndPartition = TopicAndPartition(topic, partition)
    debug("Removing replica %d from ISR %s for partition %s.".format(replicaId,
      controllerContext.partitionLeadershipInfo(topicAndPartition).leaderAndIsr.isr.mkString(","), topicAndPartition))
    var finalLeaderIsrAndControllerEpoch: Option[LeaderIsrAndControllerEpoch] = None
    var zkWriteCompleteOrUnnecessary = false
    while (!zkWriteCompleteOrUnnecessary) {
      // refresh leader and isr from zookeeper again
      val leaderIsrAndEpochOpt = ReplicationUtils.getLeaderIsrAndEpochForPartition(zkUtils, topic, partition)
      zkWriteCompleteOrUnnecessary = leaderIsrAndEpochOpt match {
        case Some(leaderIsrAndEpoch) => // increment the leader epoch even if the ISR changes
          val leaderAndIsr = leaderIsrAndEpoch.leaderAndIsr
          val controllerEpoch = leaderIsrAndEpoch.controllerEpoch
          if(controllerEpoch > epoch)
            throw new StateChangeFailedException("Leader and isr path written by another controller. This probably" +
              "means the current controller with epoch %d went through a soft failure and another ".format(epoch) +
              "controller was elected with epoch %d. Aborting state change by this controller".format(controllerEpoch))
          if (leaderAndIsr.isr.contains(replicaId)) {
            // if the replica to be removed from the ISR is also the leader, set the new leader value to -1
            val newLeader = if (replicaId == leaderAndIsr.leader) LeaderAndIsr.NoLeader else leaderAndIsr.leader
            var newIsr = leaderAndIsr.isr.filter(b => b != replicaId)

            // if the replica to be removed from the ISR is the last surviving member of the ISR and unclean leader election
            // is disallowed for the corresponding topic, then we must preserve the ISR membership so that the replica can
            // eventually be restored as the leader.
            if (newIsr.isEmpty && !LogConfig.fromProps(config.originals, AdminUtils.fetchEntityConfig(zkUtils,
              ConfigType.Topic, topicAndPartition.topic)).uncleanLeaderElectionEnable) {
              info("Retaining last ISR %d of partition %s since unclean leader election is disabled".format(replicaId, topicAndPartition))
              newIsr = leaderAndIsr.isr
            }

            val newLeaderAndIsr = leaderAndIsr.newLeaderAndIsr(newLeader, newIsr)
            // update the new leadership decision in zookeeper or retry
            val (updateSucceeded, newVersion) = ReplicationUtils.updateLeaderAndIsr(zkUtils, topic, partition,
              newLeaderAndIsr, epoch, leaderAndIsr.zkVersion)

            val leaderWithNewVersion = newLeaderAndIsr.withZkVersion(newVersion)
            finalLeaderIsrAndControllerEpoch = Some(LeaderIsrAndControllerEpoch(leaderWithNewVersion, epoch))
            controllerContext.partitionLeadershipInfo.put(topicAndPartition, finalLeaderIsrAndControllerEpoch.get)
            if (updateSucceeded) {
              info(s"New leader and ISR for partition $topicAndPartition is $leaderWithNewVersion")
            }
            updateSucceeded
          } else {
            warn(s"Cannot remove replica $replicaId from ISR of partition $topicAndPartition since it is not in the ISR." +
              s" Leader = ${leaderAndIsr.leader} ; ISR = ${leaderAndIsr.isr}")
            finalLeaderIsrAndControllerEpoch = Some(LeaderIsrAndControllerEpoch(leaderAndIsr, epoch))
            controllerContext.partitionLeadershipInfo.put(topicAndPartition, finalLeaderIsrAndControllerEpoch.get)
            true
          }
        case None =>
          warn("Cannot remove replica %d from ISR of %s - leaderAndIsr is empty.".format(replicaId, topicAndPartition))
          true
      }
    }
    finalLeaderIsrAndControllerEpoch
  }

  /**
   * Does not change leader or isr, but just increments the leader epoch
   *
   * @param topic topic
   * @param partition partition
   * @return the new leaderAndIsr with an incremented leader epoch, or None if leaderAndIsr is empty.
   */
  private def updateLeaderEpoch(topic: String, partition: Int): Option[LeaderIsrAndControllerEpoch] = {
    val topicAndPartition = TopicAndPartition(topic, partition)
    debug("Updating leader epoch for partition %s.".format(topicAndPartition))
    var finalLeaderIsrAndControllerEpoch: Option[LeaderIsrAndControllerEpoch] = None
    var zkWriteCompleteOrUnnecessary = false
    while (!zkWriteCompleteOrUnnecessary) {
      // refresh leader and isr from zookeeper again
      val leaderIsrAndEpochOpt = ReplicationUtils.getLeaderIsrAndEpochForPartition(zkUtils, topic, partition)
      zkWriteCompleteOrUnnecessary = leaderIsrAndEpochOpt match {
        case Some(leaderIsrAndEpoch) =>
          val leaderAndIsr = leaderIsrAndEpoch.leaderAndIsr
          val controllerEpoch = leaderIsrAndEpoch.controllerEpoch
          if(controllerEpoch > epoch)
            throw new StateChangeFailedException("Leader and isr path written by another controller. This probably" +
              "means the current controller with epoch %d went through a soft failure and another ".format(epoch) +
              "controller was elected with epoch %d. Aborting state change by this controller".format(controllerEpoch))
          // increment the leader epoch even if there are no leader or isr changes to allow the leader to cache the expanded
          // assigned replica list
          val newLeaderAndIsr = leaderAndIsr.newEpochAndZkVersion
          // update the new leadership decision in zookeeper or retry
          val (updateSucceeded, newVersion) = ReplicationUtils.updateLeaderAndIsr(zkUtils, topic,
            partition, newLeaderAndIsr, epoch, leaderAndIsr.zkVersion)

          val leaderWithNewVersion = newLeaderAndIsr.withZkVersion(newVersion)
          finalLeaderIsrAndControllerEpoch = Some(LeaderIsrAndControllerEpoch(leaderWithNewVersion, epoch))
          if (updateSucceeded) {
            info(s"Updated leader epoch for partition $topicAndPartition to ${leaderWithNewVersion.leaderEpoch}")
          }
          updateSucceeded
        case None =>
          throw new IllegalStateException(s"Cannot update leader epoch for partition $topicAndPartition as " +
            "leaderAndIsr path is empty. This could mean we somehow tried to reassign a partition that doesn't exist")
          true
      }
    }
    finalLeaderIsrAndControllerEpoch
  }

  private def checkAndTriggerAutoLeaderRebalance(): Unit = {
    trace("Checking need to trigger auto leader balancing")
    val preferredReplicasForTopicsByBrokers: Map[Int, Map[TopicAndPartition, Seq[Int]]] =
      controllerContext.partitionReplicaAssignment.filterNot { case (tp, _) =>
        topicDeletionManager.isTopicQueuedUpForDeletion(tp.topic)
      }.groupBy { case (_, assignedReplicas) => assignedReplicas.head }
    debug(s"Preferred replicas by broker $preferredReplicasForTopicsByBrokers")

    // for each broker, check if a preferred replica election needs to be triggered
    preferredReplicasForTopicsByBrokers.foreach { case (leaderBroker, topicAndPartitionsForBroker) =>
      val topicsNotInPreferredReplica = topicAndPartitionsForBroker.filter { case (topicPartition, _) =>
        val leadershipInfo = controllerContext.partitionLeadershipInfo.get(topicPartition)
        leadershipInfo.exists(_.leaderAndIsr.leader != leaderBroker)
      }
      debug(s"Topics not in preferred replica $topicsNotInPreferredReplica")

      val imbalanceRatio = topicsNotInPreferredReplica.size.toDouble / topicAndPartitionsForBroker.size
      trace(s"Leader imbalance ratio for broker $leaderBroker is $imbalanceRatio")

      // check ratio and if greater than desired ratio, trigger a rebalance for the topic partitions
      // that need to be on this broker
      if (imbalanceRatio > (config.leaderImbalancePerBrokerPercentage.toDouble / 100)) {
        topicsNotInPreferredReplica.keys.foreach { topicPartition =>
          // do this check only if the broker is live and there are no partitions being reassigned currently
          // and preferred replica election is not in progress
          if (controllerContext.isReplicaOnline(leaderBroker, topicPartition) &&
            controllerContext.partitionsBeingReassigned.isEmpty &&
            !topicDeletionManager.isTopicQueuedUpForDeletion(topicPartition.topic) &&
            controllerContext.allTopics.contains(topicPartition.topic)) {
            onPreferredReplicaElection(Set(topicPartition), isTriggeredByAutoRebalance = true)
          }
        }
      }
    }
  }

  def getControllerID(): Int = {
    controllerContext.zkUtils.readDataMaybeNull(ZkUtils.ControllerPath)._1 match {
      case Some(controller) => KafkaController.parseControllerId(controller)
      case None => -1
    }
  }

  case class BrokerChange(currentBrokerList: Seq[String]) extends ControllerEvent {

    def state = ControllerState.BrokerChange

    override def process(): Unit = {
      if (!isActive) return
      // Read the current broker list from ZK again instead of using currentBrokerList to increase
      // the odds of processing recent broker changes in a single ControllerEvent (KAFKA-5502).
      val curBrokers = zkUtils.getAllBrokersInCluster().toSet
      val curBrokerIds = curBrokers.map(_.id)
      val liveOrShuttingDownBrokerIds = controllerContext.liveOrShuttingDownBrokerIds
      val newBrokerIds = curBrokerIds -- liveOrShuttingDownBrokerIds
      val deadBrokerIds = liveOrShuttingDownBrokerIds -- curBrokerIds
      val newBrokers = curBrokers.filter(broker => newBrokerIds(broker.id))
      controllerContext.liveBrokers = curBrokers
      val newBrokerIdsSorted = newBrokerIds.toSeq.sorted
      val deadBrokerIdsSorted = deadBrokerIds.toSeq.sorted
      val liveBrokerIdsSorted = curBrokerIds.toSeq.sorted
      info("Newly added brokers: %s, deleted brokers: %s, all live brokers: %s"
        .format(newBrokerIdsSorted.mkString(","), deadBrokerIdsSorted.mkString(","), liveBrokerIdsSorted.mkString(",")))
      newBrokers.foreach(controllerContext.controllerChannelManager.addBroker)
      deadBrokerIds.foreach(controllerContext.controllerChannelManager.removeBroker)
      if (newBrokerIds.nonEmpty)
        onBrokerStartup(newBrokerIdsSorted)
      if (deadBrokerIds.nonEmpty)
        onBrokerFailure(deadBrokerIdsSorted)
    }
  }

  case class TopicChange(topics: Set[String]) extends ControllerEvent {

    def state = ControllerState.TopicChange

    override def process(): Unit = {
      if (!isActive) return
      val newTopics = topics -- controllerContext.allTopics
      val deletedTopics = controllerContext.allTopics -- topics
      controllerContext.allTopics = topics

      val addedPartitionReplicaAssignment = zkUtils.getReplicaAssignmentForTopics(newTopics.toSeq)
      controllerContext.partitionReplicaAssignment = controllerContext.partitionReplicaAssignment.filter(p =>
        !deletedTopics.contains(p._1.topic))
      controllerContext.partitionReplicaAssignment.++=(addedPartitionReplicaAssignment)
      info("New topics: [%s], deleted topics: [%s], new partition replica assignment [%s]".format(newTopics,
        deletedTopics, addedPartitionReplicaAssignment))
      if (newTopics.nonEmpty)
        onNewTopicCreation(newTopics, addedPartitionReplicaAssignment.keySet)
    }
  }

  case class PartitionModifications(topic: String) extends ControllerEvent {

    def state = ControllerState.TopicChange

    override def process(): Unit = {
      if (!isActive) return
      val partitionReplicaAssignment = zkUtils.getReplicaAssignmentForTopics(List(topic))
      val partitionsToBeAdded = partitionReplicaAssignment.filter(p =>
        !controllerContext.partitionReplicaAssignment.contains(p._1))
      if(topicDeletionManager.isTopicQueuedUpForDeletion(topic))
        error("Skipping adding partitions %s for topic %s since it is currently being deleted"
          .format(partitionsToBeAdded.map(_._1.partition).mkString(","), topic))
      else {
        if (partitionsToBeAdded.nonEmpty) {
          info(s"New partitions to be added $partitionsToBeAdded")
          controllerContext.partitionReplicaAssignment.++=(partitionsToBeAdded)
          onNewPartitionCreation(partitionsToBeAdded.keySet)
        }
      }
    }
  }

  case class TopicDeletion(var topicsToBeDeleted: Set[String]) extends ControllerEvent {

    def state = ControllerState.TopicDeletion

    override def process(): Unit = {
      if (!isActive) return
      debug(s"Delete topics listener fired for topics ${topicsToBeDeleted.mkString(",")} to be deleted")
      val nonExistentTopics = topicsToBeDeleted -- controllerContext.allTopics
      if (nonExistentTopics.nonEmpty) {
        warn(s"Ignoring request to delete non-existing topics ${nonExistentTopics.mkString(",")}")
        nonExistentTopics.foreach(topic => zkUtils.deletePathRecursive(getDeleteTopicPath(topic)))
      }
      topicsToBeDeleted --= nonExistentTopics
      if (config.deleteTopicEnable) {
        if (topicsToBeDeleted.nonEmpty) {
          info(s"Starting topic deletion for topics ${topicsToBeDeleted.mkString(",")}")
          // mark topic ineligible for deletion if other state changes are in progress
          topicsToBeDeleted.foreach { topic =>
            val partitionReassignmentInProgress =
              controllerContext.partitionsBeingReassigned.keySet.map(_.topic).contains(topic)
            if (partitionReassignmentInProgress)
              topicDeletionManager.markTopicIneligibleForDeletion(Set(topic))
          }
          // add topic to deletion list
          topicDeletionManager.enqueueTopicsForDeletion(topicsToBeDeleted)
        }
      } else {
        // If delete topic is disabled remove entries under zookeeper path : /admin/delete_topics
        for (topic <- topicsToBeDeleted) {
          info(s"Removing ${getDeleteTopicPath(topic)} since delete topic is disabled")
          zkUtils.deletePath(getDeleteTopicPath(topic))
        }
      }
    }
  }

  case object PartitionReassignment extends ControllerEvent {

    def state = ControllerState.PartitionReassignment

    override def process(): Unit = {
      if (!isActive) return
      // We read the reassignment data fresh so that we don't need to maintain it in memory. While
      // a reassignment is in progress, there can be potentially many reassignment events in the queue
      // since the completion of every individual reassignment causes the reassignment path to be updated.
      val partitionReassignment = zkUtils.getPartitionsBeingReassigned()
      val partitionsToBeReassigned = partitionReassignment.filterNot(p => controllerContext.partitionsBeingReassigned.contains(p._1))
      partitionsToBeReassigned.foreach { case (partition, context) =>
        if(topicDeletionManager.isTopicQueuedUpForDeletion(partition.topic)) {
          error(s"Skipping reassignment of partition $partition since it is currently being deleted")
          removePartitionFromReassignedPartitions(partition)
        } else {
          initiateReassignReplicasForTopicPartition(partition, context)
        }
      }
    }
  }

  case class PartitionReassignmentIsrChange(topicAndPartition: TopicAndPartition, reassignedReplicas: Set[Int]) extends ControllerEvent {

    def state = ControllerState.PartitionReassignment

    override def process(): Unit = {
      if (!isActive) return
        // check if this partition is still being reassigned or not
      controllerContext.partitionsBeingReassigned.get(topicAndPartition).foreach { reassignedPartitionContext =>
        // need to re-read leader and isr from zookeeper since the zkclient callback doesn't return the Stat object
        val newLeaderAndIsrOpt = zkUtils.getLeaderAndIsrForPartition(topicAndPartition.topic, topicAndPartition.partition)
        newLeaderAndIsrOpt match {
          case Some(leaderAndIsr) => // check if new replicas have joined ISR
            val caughtUpReplicas = reassignedReplicas & leaderAndIsr.isr.toSet
            if(caughtUpReplicas == reassignedReplicas) {
              // resume the partition reassignment process
              info("%d/%d replicas have caught up with the leader for partition %s being reassigned."
                .format(caughtUpReplicas.size, reassignedReplicas.size, topicAndPartition) +
                "Resuming partition reassignment")
              onPartitionReassignment(topicAndPartition, reassignedPartitionContext)
            }
            else {
              info("%d/%d replicas have caught up with the leader for partition %s being reassigned."
                .format(caughtUpReplicas.size, reassignedReplicas.size, topicAndPartition) +
                "Replica(s) %s still need to catch up".format((reassignedReplicas -- leaderAndIsr.isr.toSet).mkString(",")))
            }
          case None => error("Error handling reassignment of partition %s to replicas %s as it was never created"
            .format(topicAndPartition, reassignedReplicas.mkString(",")))
        }
      }
    }
  }

  case class IsrChangeNotification(sequenceNumbers: Seq[String]) extends ControllerEvent {

    def state = ControllerState.IsrChange

    override def process(): Unit = {
      if (!isActive) return
      try {
        val topicAndPartitions = sequenceNumbers.flatMap(getTopicAndPartition).toSet
        if (topicAndPartitions.nonEmpty) {
          updateLeaderAndIsrCache(topicAndPartitions)
          processUpdateNotifications(topicAndPartitions)
        }
      } finally {
        // delete the notifications
        sequenceNumbers.map(x => controllerContext.zkUtils.deletePath(ZkUtils.IsrChangeNotificationPath + "/" + x))
      }
    }

    private def processUpdateNotifications(topicAndPartitions: immutable.Set[TopicAndPartition]) {
      val liveBrokers: Seq[Int] = controllerContext.liveOrShuttingDownBrokerIds.toSeq
      debug("Sending MetadataRequest to Brokers:" + liveBrokers + " for TopicAndPartitions:" + topicAndPartitions)
      sendUpdateMetadataRequest(liveBrokers, topicAndPartitions)
    }

    private def getTopicAndPartition(child: String): Set[TopicAndPartition] = {
      val changeZnode = ZkUtils.IsrChangeNotificationPath + "/" + child
      val (jsonOpt, _) = controllerContext.zkUtils.readDataMaybeNull(changeZnode)
      jsonOpt.map { json =>
        Json.parseFull(json) match {
          case Some(js) =>
            val isrChanges = js.asJsonObject
            isrChanges("partitions").asJsonArray.iterator.map(_.asJsonObject).map { tpJs =>
              val topic = tpJs("topic").to[String]
              val partition = tpJs("partition").to[Int]
              TopicAndPartition(topic, partition)
            }.toSet
          case None =>
            error(s"Invalid topic and partition JSON in ZK. ZK notification node: $changeZnode, JSON: $json")
            Set.empty[TopicAndPartition]
        }
      }.getOrElse(Set.empty[TopicAndPartition])
    }

  }

  case class LogDirEventNotification(sequenceNumbers: Seq[String]) extends ControllerEvent {

    def state = ControllerState.LogDirChange

    override def process(): Unit = {
      val zkUtils = controllerContext.zkUtils
      try {
        val brokerIds = sequenceNumbers.flatMap(LogDirUtils.getBrokerIdFromLogDirEvent(zkUtils, _))
        onBrokerLogDirFailure(brokerIds)
      } finally {
        // delete processed children
        sequenceNumbers.map(x => zkUtils.deletePath(ZkUtils.LogDirEventNotificationPath + "/" + x))
      }
    }
  }

  case class PreferredReplicaLeaderElection(partitions: Set[TopicAndPartition]) extends ControllerEvent {

    def state = ControllerState.ManualLeaderBalance

    override def process(): Unit = {
      if (!isActive) return
      val partitionsForTopicsToBeDeleted = partitions.filter(p => topicDeletionManager.isTopicQueuedUpForDeletion(p.topic))
      if (partitionsForTopicsToBeDeleted.nonEmpty) {
        error("Skipping preferred replica election for partitions %s since the respective topics are being deleted"
          .format(partitionsForTopicsToBeDeleted))
      }
      onPreferredReplicaElection(partitions -- partitionsForTopicsToBeDeleted)
    }

  }

  case object AutoPreferredReplicaLeaderElection extends ControllerEvent {

    def state = ControllerState.AutoLeaderBalance

    override def process(): Unit = {
      if (!isActive) return
      try {
        checkAndTriggerAutoLeaderRebalance()
      } finally {
        scheduleAutoLeaderRebalanceTask(delay = config.leaderImbalanceCheckIntervalSeconds, unit = TimeUnit.SECONDS)
      }
    }
  }

  case class ControlledShutdown(id: Int, controlledShutdownCallback: Try[Set[TopicAndPartition]] => Unit) extends ControllerEvent {

    def state = ControllerState.ControlledShutdown

    override def process(): Unit = {
      val controlledShutdownResult = Try { doControlledShutdown(id) }
      controlledShutdownCallback(controlledShutdownResult)
    }

    private def doControlledShutdown(id: Int): Set[TopicAndPartition] = {
      if (!isActive) {
        throw new ControllerMovedException("Controller moved to another broker. Aborting controlled shutdown")
      }

      info("Shutting down broker " + id)

      if (!controllerContext.liveOrShuttingDownBrokerIds.contains(id))
        throw new BrokerNotAvailableException("Broker id %d does not exist.".format(id))

      controllerContext.shuttingDownBrokerIds.add(id)
      debug("All shutting down brokers: " + controllerContext.shuttingDownBrokerIds.mkString(","))
      debug("Live brokers: " + controllerContext.liveBrokerIds.mkString(","))

      val allPartitionsAndReplicationFactorOnBroker: Set[(TopicAndPartition, Int)] =
          controllerContext.partitionsOnBroker(id)
            .map(topicAndPartition => (topicAndPartition, controllerContext.partitionReplicaAssignment(topicAndPartition).size))

      allPartitionsAndReplicationFactorOnBroker.foreach { case (topicAndPartition, replicationFactor) =>
        controllerContext.partitionLeadershipInfo.get(topicAndPartition).foreach { currLeaderIsrAndControllerEpoch =>
          if (replicationFactor > 1) {
            if (currLeaderIsrAndControllerEpoch.leaderAndIsr.leader == id) {
              // If the broker leads the topic partition, transition the leader and update isr. Updates zk and
              // notifies all affected brokers
              partitionStateMachine.handleStateChanges(Set(topicAndPartition), OnlinePartition,
                controlledShutdownPartitionLeaderSelector)
            } else {
              // Stop the replica first. The state change below initiates ZK changes which should take some time
              // before which the stop replica request should be completed (in most cases)
              try {
                brokerRequestBatch.newBatch()
                brokerRequestBatch.addStopReplicaRequestForBrokers(Seq(id), topicAndPartition.topic,
                  topicAndPartition.partition, deletePartition = false)
                brokerRequestBatch.sendRequestsToBrokers(epoch)
              } catch {
                case e: IllegalStateException =>
                  handleIllegalState(e)
              }
              // If the broker is a follower, updates the isr in ZK and notifies the current leader
              replicaStateMachine.handleStateChanges(Set(PartitionAndReplica(topicAndPartition.topic,
                topicAndPartition.partition, id)), OfflineReplica)
            }
          }
        }
      }
      def replicatedPartitionsBrokerLeads() = {
        trace("All leaders = " + controllerContext.partitionLeadershipInfo.mkString(","))
        controllerContext.partitionLeadershipInfo.filter {
          case (topicAndPartition, leaderIsrAndControllerEpoch) =>
            leaderIsrAndControllerEpoch.leaderAndIsr.leader == id && controllerContext.partitionReplicaAssignment(topicAndPartition).size > 1
        }.keys
      }
      replicatedPartitionsBrokerLeads().toSet
    }
  }

  case class LeaderAndIsrResponseReceived(LeaderAndIsrResponseObj: AbstractResponse, brokerId: Int) extends ControllerEvent {

    def state = ControllerState.LeaderAndIsrResponseReceived

    override def process(): Unit = {
      import JavaConverters._
      val leaderAndIsrResponse = LeaderAndIsrResponseObj.asInstanceOf[LeaderAndIsrResponse]

      if (leaderAndIsrResponse.error != Errors.NONE) {
        stateChangeLogger.error(s"Received error in LeaderAndIsr response $leaderAndIsrResponse from broker $brokerId")
        return
      }

      val offlineReplicas = leaderAndIsrResponse.responses().asScala.filter(_._2 == Errors.KAFKA_STORAGE_ERROR).keys.map(
        new TopicAndPartition(_)).toSet
      val onlineReplicas = leaderAndIsrResponse.responses().asScala.filter(_._2 == Errors.NONE).keys.map(
        new TopicAndPartition(_)).toSet
      val previousOfflineReplicas = controllerContext.replicasOnOfflineDirs.getOrElse(brokerId, Set.empty[TopicAndPartition])
      val currentOfflineReplicas = previousOfflineReplicas -- onlineReplicas ++ offlineReplicas
      controllerContext.replicasOnOfflineDirs.put(brokerId, currentOfflineReplicas)
      val newOfflineReplicas = currentOfflineReplicas -- previousOfflineReplicas

      if (newOfflineReplicas.nonEmpty) {
        stateChangeLogger.info(s"Mark replicas ${newOfflineReplicas.mkString(",")} on broker $brokerId as offline")
        onReplicasBecomeOffline(newOfflineReplicas.map(tp => PartitionAndReplica(tp.topic, tp.partition, brokerId)))
      }
    }
  }

  case class TopicDeletionStopReplicaResponseReceived(stopReplicaResponseObj: AbstractResponse, replicaId: Int) extends ControllerEvent {

    def state = ControllerState.TopicDeletion

    override def process(): Unit = {
      import JavaConverters._
      if (!isActive) return
      val stopReplicaResponse = stopReplicaResponseObj.asInstanceOf[StopReplicaResponse]
      debug("Delete topic callback invoked for %s".format(stopReplicaResponse))
      val responseMap = stopReplicaResponse.responses.asScala
      val partitionsInError =
        if (stopReplicaResponse.error != Errors.NONE) responseMap.keySet
        else responseMap.filter { case (_, error) => error != Errors.NONE }.keySet
      val replicasInError = partitionsInError.map(p => PartitionAndReplica(p.topic, p.partition, replicaId))
      // move all the failed replicas to ReplicaDeletionIneligible
      topicDeletionManager.failReplicaDeletion(replicasInError)
      if (replicasInError.size != responseMap.size) {
        // some replicas could have been successfully deleted
        val deletedReplicas = responseMap.keySet -- partitionsInError
        topicDeletionManager.completeReplicaDeletion(deletedReplicas.map(p => PartitionAndReplica(p.topic, p.partition, replicaId)))
      }
    }
  }

  //controller的Startup事件
  case object Startup extends ControllerEvent {

    def state = ControllerState.ControllerChange

    override def process(): Unit = {
      //注册一个监听器，当 ZooKeeper 会话超时时，会触发相应的回调。
      registerSessionExpirationListener()
      //注册控制器变更监听器，以便在控制器角色发生变化时（例如，当前控制器失败，需要选举新的控制器）能够及时响应。
      registerControllerChangeListener()
      //尝试选举为集群的控制器
      elect()
    }
  }

  //zk的/controller节点数据变更事件
  case class ControllerChange(newControllerId: Int) extends ControllerEvent {

    def state = ControllerState.ControllerChange

    override def process(): Unit = {
      //若这个broker之前为控制器，wasActiveBeforeChange为true
      val wasActiveBeforeChange = isActive
      activeControllerId = newControllerId
      //若当前broker之前是控制前，当前已经不是了
      if (wasActiveBeforeChange && !isActive) {
        //调用控制器的"辞职"（即关闭）方法
        onControllerResignation()
      }
    }

  }

  //控制器重新选举事件
  case object Reelect extends ControllerEvent {

    def state = ControllerState.ControllerChange

    override def process(): Unit = {
      //先前broker是否为控制器
      val wasActiveBeforeChange = isActive
      //更新contrllerId为zk中/controller节点的数据
      activeControllerId = getControllerID()
      //若该broker之前是控制器，而此次更新后不再是了
      if (wasActiveBeforeChange && !isActive) {
        //调用控制器“辞职”（关闭）方法
        onControllerResignation()
      }
      //再次尝试竞选控制器
      elect()
    }

  }

  private def updateMetrics(): Unit = {
    offlinePartitionCount =
      if (!isActive) {
        0
      } else {
        controllerContext.partitionLeadershipInfo.count { case (tp, leadershipInfo) =>
          !controllerContext.liveOrShuttingDownBrokerIds.contains(leadershipInfo.leaderAndIsr.leader) &&
            !topicDeletionManager.isTopicQueuedUpForDeletion(tp.topic)
        }
      }

    preferredReplicaImbalanceCount =
      if (!isActive) {
        0
      } else {
        controllerContext.partitionReplicaAssignment.count { case (topicPartition, replicas) =>
          val preferredReplica = replicas.head
          val leadershipInfo = controllerContext.partitionLeadershipInfo.get(topicPartition)
          leadershipInfo.map(_.leaderAndIsr.leader != preferredReplica).getOrElse(false) &&
            !topicDeletionManager.isTopicQueuedUpForDeletion(topicPartition.topic)
        }
      }

    globalTopicCount = if (!isActive) 0 else controllerContext.allTopics.size

    globalPartitionCount = if (!isActive) 0 else controllerContext.partitionLeadershipInfo.size
  }

  // visible for testing
  private[controller] def handleIllegalState(e: IllegalStateException): Nothing = {
    // Resign if the controller is in an illegal state
    error("Forcing the controller to resign")
    brokerRequestBatch.clear()
    triggerControllerMove()
    throw e
  }

  private def triggerControllerMove(): Unit = {
    onControllerResignation()
    activeControllerId = -1
    controllerContext.zkUtils.deletePath(ZkUtils.ControllerPath)
  }

  //尝试竞选为控制器的方法
  def elect(): Unit = {
    val timestamp = time.milliseconds
    val electString = ZkUtils.controllerZkData(config.brokerId, timestamp)

    //获取zk的/controller节点数据，并解析出控制器的brokerId
    activeControllerId = getControllerID()
    /*
     * We can get here during the initial startup and the handleDeleted ZK callback. Because of the potential race condition,
     * it's possible that the controller has already been elected when we get here. This check will prevent the following
     * createEphemeralPath method from getting into an infinite loop if this broker is already the controller.
     */
    //说明集群已存在controler，直接返回
    if (activeControllerId != -1) {
      debug("Broker %d has been elected as the controller, so stopping the election process.".format(activeControllerId))
      return
    }

    //否则抢占式方式尝试在zk中创建/controller的临时节点
    try {
      val zkCheckedEphemeral = new ZKCheckedEphemeral(ZkUtils.ControllerPath,
                                                      electString,
                                                      controllerContext.zkUtils.zkConnection.getZookeeper,
                                                      controllerContext.zkUtils.isSecure)
      zkCheckedEphemeral.create()
      //未抛出异常，竞选成功
      info(config.brokerId + " successfully elected as the controller")
      //设置为当前的broker.id
      activeControllerId = config.brokerId
      //控制器初始化方法
      onControllerFailover()
    } catch {
      case _: ZkNodeExistsException =>
        // If someone else has written the path, then
        activeControllerId = getControllerID

        if (activeControllerId != -1)
          debug("Broker %d was elected as controller instead of broker %d".format(activeControllerId, config.brokerId))
        else
          warn("A controller has been elected but just resigned, this will result in another round of election")

      case e2: Throwable =>
        error("Error while electing or becoming controller on broker %d".format(config.brokerId), e2)
        triggerControllerMove()
    }
  }
}

/**
  * This is the zookeeper listener that triggers all the state transitions for a replica
  */
class BrokerChangeListener(controller: KafkaController, eventManager: ControllerEventManager) extends IZkChildListener with Logging {
  override def handleChildChange(parentPath: String, currentChilds: java.util.List[String]): Unit = {
    import JavaConverters._
    eventManager.put(controller.BrokerChange(currentChilds.asScala))
  }
}

class TopicChangeListener(controller: KafkaController, eventManager: ControllerEventManager) extends IZkChildListener with Logging {
  override def handleChildChange(parentPath: String, currentChilds: java.util.List[String]): Unit = {
    import JavaConverters._
    eventManager.put(controller.TopicChange(currentChilds.asScala.toSet))
  }
}

/**
  * Called when broker notifies controller of log directory change
  */
class LogDirEventNotificationListener(controller: KafkaController, eventManager: ControllerEventManager) extends IZkChildListener with Logging {
  override def handleChildChange(parentPath: String, currentChilds: java.util.List[String]): Unit = {
    import JavaConverters._
    eventManager.put(controller.LogDirEventNotification(currentChilds.asScala))
  }
}

object LogDirEventNotificationListener {
  val version: Long = 1L
}

class PartitionModificationsListener(controller: KafkaController, eventManager: ControllerEventManager, topic: String) extends IZkDataListener with Logging {
  override def handleDataChange(dataPath: String, data: Any): Unit = {
    eventManager.put(controller.PartitionModifications(topic))
  }

  override def handleDataDeleted(dataPath: String): Unit = {}
}

/**
  * Delete topics includes the following operations -
  * 1. Add the topic to be deleted to the delete topics cache, only if the topic exists
  * 2. If there are topics to be deleted, it signals the delete topic thread
  */
class TopicDeletionListener(controller: KafkaController, eventManager: ControllerEventManager) extends IZkChildListener with Logging {
  override def handleChildChange(parentPath: String, currentChilds: java.util.List[String]): Unit = {
    import JavaConverters._
    eventManager.put(controller.TopicDeletion(currentChilds.asScala.toSet))
  }
}

/**
 * Starts the partition reassignment process unless -
 * 1. Partition previously existed
 * 2. New replicas are the same as existing replicas
 * 3. Any replica in the new set of replicas are dead
 * If any of the above conditions are satisfied, it logs an error and removes the partition from list of reassigned
 * partitions.
 */
class PartitionReassignmentListener(controller: KafkaController, eventManager: ControllerEventManager) extends IZkDataListener with Logging {
  override def handleDataChange(dataPath: String, data: Any): Unit = {
    eventManager.put(controller.PartitionReassignment)
  }

  override def handleDataDeleted(dataPath: String): Unit = {}
}

class PartitionReassignmentIsrChangeListener(controller: KafkaController, eventManager: ControllerEventManager,
                                             topic: String, partition: Int, reassignedReplicas: Set[Int]) extends IZkDataListener with Logging {
  override def handleDataChange(dataPath: String, data: Any): Unit = {
    eventManager.put(controller.PartitionReassignmentIsrChange(TopicAndPartition(topic, partition), reassignedReplicas))
  }

  override def handleDataDeleted(dataPath: String): Unit = {}
}

/**
 * Called when replica leader initiates isr change
 */
class IsrChangeNotificationListener(controller: KafkaController, eventManager: ControllerEventManager) extends IZkChildListener with Logging {
  override def handleChildChange(parentPath: String, currentChilds: java.util.List[String]): Unit = {
    import JavaConverters._
    eventManager.put(controller.IsrChangeNotification(currentChilds.asScala))
  }
}

object IsrChangeNotificationListener {
  val version: Long = 1L
}

/**
 * Starts the preferred replica leader election for the list of partitions specified under
 * /admin/preferred_replica_election -
 */
class PreferredReplicaElectionListener(controller: KafkaController, eventManager: ControllerEventManager) extends IZkDataListener with Logging {
  override def handleDataChange(dataPath: String, data: Any): Unit = {
    val partitions = PreferredReplicaLeaderElectionCommand.parsePreferredReplicaElectionData(data.toString)
    eventManager.put(controller.PreferredReplicaLeaderElection(partitions))
  }

  override def handleDataDeleted(dataPath: String): Unit = {}
}

class ControllerChangeListener(controller: KafkaController, eventManager: ControllerEventManager) extends IZkDataListener {
  //zk的/controller节点数据变更
  override def handleDataChange(dataPath: String, data: Any): Unit = {
    eventManager.put(controller.ControllerChange(KafkaController.parseControllerId(data.toString)))
  }

  //zk的/controler节点被删除，则触发重新选举
  override def handleDataDeleted(dataPath: String): Unit = {
    eventManager.put(controller.Reelect)
  }
}

class SessionExpirationListener(controller: KafkaController, eventManager: ControllerEventManager) extends IZkStateListener with Logging {
  override def handleStateChanged(state: KeeperState) {
    // do nothing, since zkclient will do reconnect for us.
  }

  /**
    * Called after the zookeeper session has expired and a new session has been created. You would have to re-create
    * any ephemeral nodes here.
    *
    * @throws Exception On any error.
    */
  @throws[Exception]
  override def handleNewSession(): Unit = {
    eventManager.put(controller.Reelect)
  }

  override def handleSessionEstablishmentError(error: Throwable): Unit = {
    //no-op handleSessionEstablishmentError in KafkaHealthCheck should handle this error in its handleSessionEstablishmentError
  }
}

case class ReassignedPartitionsContext(var newReplicas: Seq[Int] = Seq.empty,
                                       var isrChangeListener: PartitionReassignmentIsrChangeListener = null)

case class PartitionAndReplica(topic: String, partition: Int, replica: Int) {
  override def toString: String = {
    "[Topic=%s,Partition=%d,Replica=%d]".format(topic, partition, replica)
  }
}

case class LeaderIsrAndControllerEpoch(leaderAndIsr: LeaderAndIsr, controllerEpoch: Int) {
  override def toString: String = {
    val leaderAndIsrInfo = new StringBuilder
    leaderAndIsrInfo.append("(Leader:" + leaderAndIsr.leader)
    leaderAndIsrInfo.append(",ISR:" + leaderAndIsr.isr.mkString(","))
    leaderAndIsrInfo.append(",LeaderEpoch:" + leaderAndIsr.leaderEpoch)
    leaderAndIsrInfo.append(",ControllerEpoch:" + controllerEpoch + ")")
    leaderAndIsrInfo.toString()
  }
}

private[controller] class ControllerStats extends KafkaMetricsGroup {
  val uncleanLeaderElectionRate = newMeter("UncleanLeaderElectionsPerSec", "elections", TimeUnit.SECONDS)

  val rateAndTimeMetrics: Map[ControllerState, KafkaTimer] = ControllerState.values.flatMap { state =>
    state.rateAndTimeMetricName.map { metricName =>
      state -> new KafkaTimer(newTimer(s"$metricName", TimeUnit.MILLISECONDS, TimeUnit.SECONDS))
    }
  }.toMap

}

sealed trait ControllerEvent {
  def state: ControllerState
  def process(): Unit
}
