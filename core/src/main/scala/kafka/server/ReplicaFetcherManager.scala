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

package kafka.server

import kafka.cluster.BrokerEndPoint
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.utils.Time

//在 Apache Kafka 中，`replicaFetcherManager` 是一个关键组件，负责管理 Follower 副本从 Leader 副本拉取数据的过程。以下是关于 `replicaFetcherManager` 的一些详细信息：
//
//1. `replicaFetcherManager` 是在 Kafka 集群启动时创建的，它负责为 Follower 分区添加数据拉取任务，确保 Follower 副本能够从 Leader 副本同步数据 。
//
//2. 该管理器使用 `addFetcherForPartitions` 方法为特定的分区添加拉取任务。这个方法首先会根据分区和偏移量创建一个映射，然后为每个映射创建一个拉取线程，这些线程会不断地从 Leader 副本拉取数据并更新 Follower 副本 。
//
//3. 在 Follower 副本成为 Leader 的过程中，`replicaFetcherManager` 也会参与其中。例如，在 `ReplicaManager.makeFollowers` 函数中，会调用 `replicaFetcherManager.addFetcherForPartitions` 方法，将 Follower 分区纳入管理，并开始同步 Leader 中的日志到 Follower 中 。
//
//4. `replicaFetcherManager` 还涉及到性能监控，它在 Kafka 的监控指标中注册了相关性能指标，这些指标可以在 `kafka.server:type=ReplicaFetcherManager` 组中找到 。
//
//5. 创建 `ReplicaFetcherThread` 的 `createFetcherThread` 方法会为每个拉取任务创建一个线程，线程名称会包含线程前缀（如果有定义的话）、fetcherId 和源代理的 ID 。
//
//6. 当 Kafka 集群需要关闭或重新平衡时，`replicaFetcherManager` 的 `shutdown` 方法会被调用，以确保所有拉取任务能够安全地停止 。
//
//通过这些信息，我们可以看出 `replicaFetcherManager` 在 Kafka 的数据复制和同步中扮演着至关重要的角色，确保了数据在集群中的高可用性和一致性。
class ReplicaFetcherManager(brokerConfig: KafkaConfig, protected val replicaManager: ReplicaManager, metrics: Metrics,
                            time: Time, threadNamePrefix: Option[String] = None, quotaManager: ReplicationQuotaManager)
      extends AbstractFetcherManager("ReplicaFetcherManager on broker " + brokerConfig.brokerId,
        "Replica", brokerConfig.numReplicaFetchers) {

  override def createFetcherThread(fetcherId: Int, sourceBroker: BrokerEndPoint): AbstractFetcherThread = {
    val prefix = threadNamePrefix.map(tp => s"${tp}:").getOrElse("")
    val threadName = s"${prefix}ReplicaFetcherThread-$fetcherId-${sourceBroker.id}"
    new ReplicaFetcherThread(threadName, fetcherId, sourceBroker, brokerConfig, replicaManager, metrics, time, quotaManager)
  }

  def shutdown() {
    info("shutting down")
    closeAllFetchers()
    info("shutdown completed")
  }
}
