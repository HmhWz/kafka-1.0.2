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

package kafka.log

import java.io._
import java.nio.file.Files
import java.util.concurrent._

import com.yammer.metrics.core.Gauge
import kafka.admin.AdminUtils
import kafka.common.KafkaException
import kafka.metrics.KafkaMetricsGroup
import kafka.server.checkpoints.OffsetCheckpointFile
import kafka.server.{BrokerState, RecoveringFromUncleanShutdown, _}
import kafka.utils._
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.utils.Time
import org.apache.kafka.common.errors.{LogDirNotFoundException, KafkaStorageException}

import scala.collection.JavaConverters._
import scala.collection._
import scala.collection.mutable.ArrayBuffer

/**
 * The entry point to the kafka log management subsystem. The log manager is responsible for log creation, retrieval, and cleaning.
 * All read and write operations are delegated to the individual log instances.
 *
 * The log manager maintains logs in one or more directories. New logs are created in the data directory
 * with the fewest logs. No attempt is made to move partitions after the fact or balance based on
 * size or I/O rate.
 *
 * A background thread handles log retention by periodically truncating excess log segments.
 */
@threadsafe
class LogManager(logDirs: Seq[File],
                 initialOfflineDirs: Seq[File],
                 val topicConfigs: Map[String, LogConfig], // note that this doesn't get updated after creation
                 val defaultConfig: LogConfig,
                 val cleanerConfig: CleanerConfig,
                 ioThreads: Int,
                 val flushCheckMs: Long,
                 val flushRecoveryOffsetCheckpointMs: Long,
                 val flushStartOffsetCheckpointMs: Long,
                 val retentionCheckMs: Long,
                 val maxPidExpirationMs: Int,
                 scheduler: Scheduler,
                 val brokerState: BrokerState,
                 brokerTopicStats: BrokerTopicStats,
                 logDirFailureChannel: LogDirFailureChannel,
                 time: Time) extends Logging with KafkaMetricsGroup {

  import LogManager._

  val LockFile = ".lock"
  val InitialTaskDelayMs = 30 * 1000

  private val logCreationOrDeletionLock = new Object
  private val logs = new Pool[TopicPartition, Log]()
  private val logsToBeDeleted = new LinkedBlockingQueue[Log]()

  //有效日志目录
  private val _liveLogDirs: ConcurrentLinkedQueue[File] = createAndValidateLogDirs(logDirs, initialOfflineDirs)

  def liveLogDirs: Seq[File] = {
    if (_liveLogDirs.size == logDirs.size)
      logDirs
    else
      _liveLogDirs.asScala.toBuffer
  }

  private val dirLocks = lockLogDirs(liveLogDirs)
  @volatile private var recoveryPointCheckpoints = liveLogDirs.map(dir =>
    (dir, new OffsetCheckpointFile(new File(dir, RecoveryPointCheckpointFile), logDirFailureChannel))).toMap
  @volatile private var logStartOffsetCheckpoints = liveLogDirs.map(dir =>
    (dir, new OffsetCheckpointFile(new File(dir, LogStartOffsetCheckpointFile), logDirFailureChannel))).toMap

  private val preferredLogDirs = new ConcurrentHashMap[TopicPartition, String]()

  private def offlineLogDirs: Iterable[File] = {
    val logDirsSet = mutable.Set[File](logDirs: _*)
    _liveLogDirs.asScala.foreach(logDirsSet -=)
    logDirsSet
  }

  //加载日志目录中的日志文件。这个方法在 LogManager 启动时被调用，以确保所有现存的日志都被加载到内存中，从而可以进行读写操作。
  loadLogs()


  // public, so we can access this from kafka.admin.DeleteTopicTest
  val cleaner: LogCleaner =
    if(cleanerConfig.enableCleaner)
      new LogCleaner(cleanerConfig, liveLogDirs, logs, logDirFailureChannel, time = time)
    else
      null

  val offlineLogDirectoryCount = newGauge(
    "OfflineLogDirectoryCount",
    new Gauge[Int] {
      def value = offlineLogDirs.size
    }
  )

  for (dir <- logDirs) {
    newGauge(
      "LogDirectoryOffline",
      new Gauge[Int] {
        def value = if (_liveLogDirs.contains(dir)) 0 else 1
      },
      Map("logDirectory" -> dir.getAbsolutePath)
    )
  }

  /**
   * Create and check validity of the given directories that are not in the given offline directories, specifically:
   * <ol>
   * <li> Ensure that there are no duplicates in the directory list
   * <li> Create each directory if it doesn't exist
   * <li> Check that each path is a readable directory
   * </ol>
   */
  private def createAndValidateLogDirs(dirs: Seq[File], initialOfflineDirs: Seq[File]): ConcurrentLinkedQueue[File] = {
    //检查重复的日志目录，如果有重复，会抛出异常，因为每个日志目录应该是唯一的。
    if(dirs.map(_.getCanonicalPath).toSet.size < dirs.size)
      throw new KafkaException("Duplicate log directory found: " + dirs.mkString(", "))

    val liveLogDirs = new ConcurrentLinkedQueue[File]()

    for (dir <- dirs if !initialOfflineDirs.contains(dir)) {
      try {
        //遍历日志目录列表，如果发现某个目录不存在，方法会尝试创建它。如果创建失败，会抛出异常。
        if (!dir.exists) {
          info("Log directory '" + dir.getAbsolutePath + "' not found, creating it.")
          val created = dir.mkdirs()
          if (!created)
            throw new IOException("Failed to create data directory " + dir.getAbsolutePath)
        }
        //如果目录不可读，同样会抛出异常。
        if (!dir.isDirectory || !dir.canRead)
          throw new IOException(dir.getAbsolutePath + " is not a readable log directory.")
        liveLogDirs.add(dir)
      } catch {
        case e: IOException =>
          error(s"Failed to create or validate data directory $dir.getAbsolutePath", e)
      }
    }
    //若没有一个有效日志目录，直接退出进程
    if (liveLogDirs.isEmpty) {
      fatal(s"Shutdown broker because none of the specified log dirs from " + dirs.mkString(", ") + " can be created or validated")
      Exit.halt(1)
    }
    //返回一个包含所有有效日志目录的集合。这个集合将被用于 LogManager 的后续操作
    liveLogDirs
  }

  // dir should be an absolute path
  def handleLogDirFailure(dir: String) {
    info(s"Stopping serving logs in dir $dir")
    logCreationOrDeletionLock synchronized {
      _liveLogDirs.remove(new File(dir))
      if (_liveLogDirs.isEmpty) {
        fatal(s"Shutdown broker because all log dirs in ${logDirs.mkString(", ")} have failed")
        Exit.halt(1)
      }

      recoveryPointCheckpoints = recoveryPointCheckpoints.filter { case (file, _) => file.getAbsolutePath != dir }
      logStartOffsetCheckpoints = logStartOffsetCheckpoints.filter { case (file, _) => file.getAbsolutePath != dir }
      if (cleaner != null)
        cleaner.handleLogDirFailure(dir)

      val offlineTopicPartitions = logs.collect {
        case (tp, log) if log.dir.getParent == dir => tp
      }

      offlineTopicPartitions.foreach { topicPartition =>
        val removedLog = logs.remove(topicPartition)
        if (removedLog != null) {
          removedLog.closeHandlers()
          removedLog.removeLogMetrics()
        }
      }
      info(s"Partitions ${offlineTopicPartitions.mkString(",")} are offline due to failure on log directory $dir")
      dirLocks.filter(_.file.getParent == dir).foreach(dir => CoreUtils.swallow(dir.destroy()))
    }
  }

  /**
   * Lock all the given directories
   */
  private def lockLogDirs(dirs: Seq[File]): Seq[FileLock] = {
    dirs.flatMap { dir =>
      try {
        val lock = new FileLock(new File(dir, LockFile))
        if (!lock.tryLock())
          throw new KafkaException("Failed to acquire lock on file .lock in " + lock.file.getParent +
            ". A Kafka instance in another process or thread is using this directory.")
        Some(lock)
      } catch {
        case e: IOException =>
          logDirFailureChannel.maybeAddOfflineLogDir(dir.getAbsolutePath, s"Disk error while locking directory $dir", e)
          None
      }
    }
  }

  private def loadLog(logDir: File, recoveryPoints: Map[TopicPartition, Long], logStartOffsets: Map[TopicPartition, Long]): Unit = {
    debug("Loading log '" + logDir.getName + "'")
    //从日志目录名称中解析出topic和partition
    val topicPartition = Log.parseTopicPartitionName(logDir)
    val config = topicConfigs.getOrElse(topicPartition.topic, defaultConfig)
    val logRecoveryPoint = recoveryPoints.getOrElse(topicPartition, 0L)
    val logStartOffset = logStartOffsets.getOrElse(topicPartition, 0L)

    //建 Log 对象后，初始化时会加载所有的 segment文件
    val current = Log(
      dir = logDir,
      config = config,
      logStartOffset = logStartOffset,
      recoveryPoint = logRecoveryPoint,
      maxProducerIdExpirationMs = maxPidExpirationMs,
      producerIdExpirationCheckIntervalMs = LogManager.ProducerIdExpirationCheckIntervalMs,
      scheduler = scheduler,
      time = time,
      brokerTopicStats = brokerTopicStats,
      logDirFailureChannel = logDirFailureChannel)

    //如果日志目录的名称以 -delete 结尾，表示该日志已被标记为删除，将其添加到待删除列表中
    if (logDir.getName.endsWith(Log.DeleteDirSuffix)) {
      this.logsToBeDeleted.add(current)
    } else {
      val previous = this.logs.put(topicPartition, current)
      if (previous != null) {
        throw new IllegalArgumentException(
          "Duplicate log directories found: %s, %s!".format(
            current.dir.getAbsolutePath, previous.dir.getAbsolutePath))
      }
    }
  }

  /**
   * Recover and load all logs in the given data directories
   */
  private def loadLogs(): Unit = {
    info("Loading logs.")
    val startMs = time.milliseconds
    val threadPools = ArrayBuffer.empty[ExecutorService]
    val offlineDirs = ArrayBuffer.empty[(String, IOException)]
    val jobs = mutable.Map.empty[File, Seq[Future[_]]]

    for (dir <- liveLogDirs) {
      try {
        //为每个log.dir日志目录创建一个固定大小的线程池，以便并发地加载日志目录。
        val pool = Executors.newFixedThreadPool(ioThreads)
        threadPools.append(pool)

        //检查每个log.dir日志目录下是否存在 .kafka_cleanshutdown 文件，该文件表明 Kafka 上次是正常关闭的，从而跳过恢复过程
        val cleanShutdownFile = new File(dir, Log.CleanShutdownFile)

        if (cleanShutdownFile.exists) {
          debug(s"Found clean shutdown file. Skipping recovery for all logs in data directory: ${dir.getAbsolutePath}")
        } else {
          //否则，说明需要加载日志文件，这里修改broker状态
          // log recovery itself is being performed by `Log` class during initialization
          brokerState.newState(RecoveringFromUncleanShutdown)
        }

        //尝试读取每个日志目录下的 recovery-point-offset-checkpoint 文件，这个文件存储了每个日志分区的恢复点偏移量
        var recoveryPoints = Map[TopicPartition, Long]()
        try {
          recoveryPoints = this.recoveryPointCheckpoints(dir).read
        } catch {
          case e: Exception =>
            warn("Error occurred while reading recovery-point-offset-checkpoint file of directory " + dir, e)
            warn("Resetting the recovery checkpoint to 0")
        }

        var logStartOffsets = Map[TopicPartition, Long]()
        try {
          logStartOffsets = this.logStartOffsetCheckpoints(dir).read
        } catch {
          case e: Exception =>
            warn("Error occurred while reading log-start-offset-checkpoint file of directory " + dir, e)
        }

        val jobsForDir = for {
          //遍历每个日志目录下的所有文件，得到每个以 topic-partition 命名的目录
          dirContent <- Option(dir.listFiles).toList
          logDir <- dirContent if logDir.isDirectory
        } yield {
          //加载每个分区目录都将创建一个线程
          CoreUtils.runnable {
            try {
              loadLog(logDir, recoveryPoints, logStartOffsets)
            } catch {
              case e: IOException =>
                offlineDirs.append((dir.getAbsolutePath, e))
                error("Error while loading log dir " + dir.getAbsolutePath, e)
            }
          }
        }
        //将所有用于分区日志加载的job提交到线程池
        //注意这里jobs是一个map，key是.kafka_cleanshutdown文件路径，value是一个future列表
        jobs(cleanShutdownFile) = jobsForDir.map(pool.submit)
      } catch {
        case e: IOException =>
          offlineDirs.append((dir.getAbsolutePath, e))
          error("Error while loading log dir " + dir.getAbsolutePath, e)
      }
    }

    try {
      for ((cleanShutdownFile, dirJobs) <- jobs) {
        //等待所有线程池中的日志加载任务完成，确保所有日志都已加载
        dirJobs.foreach(_.get)
        try {
          //删除 .kafka_cleanshutdown 文件，因为它只在启动时检查一次，之后就不再需要
          cleanShutdownFile.delete()
        } catch {
          case e: IOException =>
            offlineDirs.append((cleanShutdownFile.getParent, e))
            error(s"Error while deleting the clean shutdown file $cleanShutdownFile", e)
        }
      }
      offlineDirs.foreach { case (dir, e) =>
        logDirFailureChannel.maybeAddOfflineLogDir(dir, s"Error while deleting the clean shutdown file in dir $dir", e)
      }
    } catch {
      case e: ExecutionException =>
        error("There was an error in one of the threads during logs loading: " + e.getCause)
        throw e.getCause
    } finally {
      //关闭线程池
      threadPools.foreach(_.shutdown())
    }

    //记录加载所花费的时间
    info(s"Logs loading complete in ${time.milliseconds - startMs} ms.")
  }

  /**
   *  Start the background threads to flush logs and do log cleanup
   */
  def startup() {
    /* Schedule the cleanup task to delete old logs */
    if (scheduler != null) {
      info("Starting log cleanup with a period of %d ms.".format(retentionCheckMs))
      scheduler.schedule("kafka-log-retention",
                         cleanupLogs _,
                         delay = InitialTaskDelayMs,
                         period = retentionCheckMs,
                         TimeUnit.MILLISECONDS)
      info("Starting log flusher with a default period of %d ms.".format(flushCheckMs))
      scheduler.schedule("kafka-log-flusher",
                         flushDirtyLogs _,
                         delay = InitialTaskDelayMs,
                         period = flushCheckMs,
                         TimeUnit.MILLISECONDS)
      scheduler.schedule("kafka-recovery-point-checkpoint",
                         checkpointLogRecoveryOffsets _,
                         delay = InitialTaskDelayMs,
                         period = flushRecoveryOffsetCheckpointMs,
                         TimeUnit.MILLISECONDS)
      scheduler.schedule("kafka-log-start-offset-checkpoint",
                         checkpointLogStartOffsets _,
                         delay = InitialTaskDelayMs,
                         period = flushStartOffsetCheckpointMs,
                         TimeUnit.MILLISECONDS)
      scheduler.schedule("kafka-delete-logs",
                         deleteLogs _,
                         delay = InitialTaskDelayMs,
                         period = defaultConfig.fileDeleteDelayMs,
                         TimeUnit.MILLISECONDS)
    }
    if (cleanerConfig.enableCleaner)
      cleaner.startup()
  }

  /**
   * Close all the logs
   */
  def shutdown() {
    info("Shutting down.")

    removeMetric("OfflineLogDirectoryCount")
    for (dir <- logDirs) {
      removeMetric("LogDirectoryOffline", Map("logDirectory" -> dir.getAbsolutePath))
    }

    val threadPools = ArrayBuffer.empty[ExecutorService]
    val jobs = mutable.Map.empty[File, Seq[Future[_]]]

    // stop the cleaner first
    if (cleaner != null) {
      CoreUtils.swallow(cleaner.shutdown())
    }

    // close logs in each dir
    for (dir <- liveLogDirs) {
      debug("Flushing and closing logs at " + dir)

      val pool = Executors.newFixedThreadPool(ioThreads)
      threadPools.append(pool)

      val logsInDir = logsByDir.getOrElse(dir.toString, Map()).values

      val jobsForDir = logsInDir map { log =>
        CoreUtils.runnable {
          // flush the log to ensure latest possible recovery point
          log.flush()
          log.close()
        }
      }

      jobs(dir) = jobsForDir.map(pool.submit).toSeq
    }

    try {
      for ((dir, dirJobs) <- jobs) {
        dirJobs.foreach(_.get)

        // update the last flush point
        debug("Updating recovery points at " + dir)
        checkpointLogRecoveryOffsetsInDir(dir)

        debug("Updating log start offsets at " + dir)
        checkpointLogStartOffsetsInDir(dir)

        // mark that the shutdown was clean by creating marker file
        debug("Writing clean shutdown marker at " + dir)
        CoreUtils.swallow(Files.createFile(new File(dir, Log.CleanShutdownFile).toPath))
      }
    } catch {
      case e: ExecutionException =>
        error("There was an error in one of the threads during LogManager shutdown: " + e.getCause)
        throw e.getCause
    } finally {
      threadPools.foreach(_.shutdown())
      // regardless of whether the close succeeded, we need to unlock the data directories
      dirLocks.foreach(_.destroy())
    }

    info("Shutdown complete.")
  }

  /**
   * Truncate the partition logs to the specified offsets and checkpoint the recovery point to this offset
   *
   * @param partitionOffsets Partition logs that need to be truncated
   */
  def truncateTo(partitionOffsets: Map[TopicPartition, Long]) {
    var truncated = false
    for ((topicPartition, truncateOffset) <- partitionOffsets) {
      val log = logs.get(topicPartition)
      // If the log does not exist, skip it
      if (log != null) {
        //May need to abort and pause the cleaning of the log, and resume after truncation is done.
        val needToStopCleaner = cleaner != null && truncateOffset < log.activeSegment.baseOffset
        if (needToStopCleaner)
          cleaner.abortAndPauseCleaning(topicPartition)
        try {
          if (log.truncateTo(truncateOffset))
            truncated = true
          if (needToStopCleaner)
            cleaner.maybeTruncateCheckpoint(log.dir.getParentFile, topicPartition, log.activeSegment.baseOffset)
        } finally {
          if (needToStopCleaner)
            cleaner.resumeCleaning(topicPartition)
        }
      }
    }

    if (truncated)
      checkpointLogRecoveryOffsets()
  }

  /**
   *  Delete all data in a partition and start the log at the new offset
   *
   *  @param newOffset The new offset to start the log with
   */
  def truncateFullyAndStartAt(topicPartition: TopicPartition, newOffset: Long) {
    val log = logs.get(topicPartition)
    // If the log does not exist, skip it
    if (log != null) {
        //Abort and pause the cleaning of the log, and resume after truncation is done.
      if (cleaner != null)
        cleaner.abortAndPauseCleaning(topicPartition)
      log.truncateFullyAndStartAt(newOffset)
      if (cleaner != null) {
        cleaner.maybeTruncateCheckpoint(log.dir.getParentFile, topicPartition, log.activeSegment.baseOffset)
        cleaner.resumeCleaning(topicPartition)
      }
    }
    checkpointLogRecoveryOffsets()
  }

  /**
   * Write out the current recovery point for all logs to a text file in the log directory
   * to avoid recovering the whole log on startup.
   */
  def checkpointLogRecoveryOffsets() {
    liveLogDirs.foreach(checkpointLogRecoveryOffsetsInDir)
  }

  /**
   * Write out the current log start offset for all logs to a text file in the log directory
   * to avoid exposing data that have been deleted by DeleteRecordsRequest
   */
  def checkpointLogStartOffsets() {
    liveLogDirs.foreach(checkpointLogStartOffsetsInDir)
  }

  /**
   * Make a checkpoint for all logs in provided directory.
   */
  private def checkpointLogRecoveryOffsetsInDir(dir: File): Unit = {
    for {
      partitionToLog <- logsByDir.get(dir.getAbsolutePath)
      checkpoint <- recoveryPointCheckpoints.get(dir)
    } {
      try {
        checkpoint.write(partitionToLog.mapValues(_.recoveryPoint))
        logs.values.foreach(_.deleteSnapshotsAfterRecoveryPointCheckpoint())
      } catch {
        case e: IOException =>
          logDirFailureChannel.maybeAddOfflineLogDir(dir.getAbsolutePath, s"Disk error while writing to recovery point " +
            s"file in directory $dir", e)
      }
    }
  }

  /**
   * Checkpoint log start offset for all logs in provided directory.
   */
  private def checkpointLogStartOffsetsInDir(dir: File): Unit = {
    for {
      partitionToLog <- logsByDir.get(dir.getAbsolutePath)
      checkpoint <- logStartOffsetCheckpoints.get(dir)
    } {
      try {
        val logStartOffsets = partitionToLog.filter { case (_, log) =>
          log.logStartOffset > log.logSegments.head.baseOffset
        }.mapValues(_.logStartOffset)
        checkpoint.write(logStartOffsets)
      } catch {
        case e: IOException =>
          logDirFailureChannel.maybeAddOfflineLogDir(dir.getAbsolutePath, s"Disk error while writing to logStartOffset file in directory $dir", e)
      }
    }
  }

  def updatePreferredLogDir(topicPartition: TopicPartition, logDir: String): Unit = {
    // The logDir should be an absolute path
    preferredLogDirs.put(topicPartition, logDir)
  }

  /**
   * Get the log if it exists, otherwise return None
   */
  def getLog(topicPartition: TopicPartition): Option[Log] = Option(logs.get(topicPartition))

  /**
   * If the log already exists, just return a copy of the existing log
   * Otherwise if isNew=true or if there is no offline log directory, create a log for the given topic and the given partition
   * Otherwise throw KafkaStorageException
   *
   * @param isNew Whether the replica should have existed on the broker or not
   * @throws KafkaStorageException if isNew=false, log is not found in the cache and there is offline log directory on the broker
   */
  def getOrCreateLog(topicPartition: TopicPartition, config: LogConfig, isNew: Boolean = false): Log = {
    logCreationOrDeletionLock synchronized {
      getLog(topicPartition).getOrElse {
        // create the log if it has not already been created in another thread
        if (!isNew && offlineLogDirs.nonEmpty)
          throw new KafkaStorageException(s"Can not create log for $topicPartition because log directories ${offlineLogDirs.mkString(",")} are offline")

        val logDir = {
          val preferredLogDir = preferredLogDirs.get(topicPartition)
          if (preferredLogDir != null)
            preferredLogDir
          else
            nextLogDir().getAbsolutePath
        }
        if (!isLogDirOnline(logDir))
          throw new KafkaStorageException(s"Can not create log for $topicPartition because log directory $logDir is offline")

        try {
          val dir = new File(logDir, topicPartition.topic + "-" + topicPartition.partition)
          Files.createDirectories(dir.toPath)

          val log = Log(
            dir = dir,
            config = config,
            logStartOffset = 0L,
            recoveryPoint = 0L,
            maxProducerIdExpirationMs = maxPidExpirationMs,
            producerIdExpirationCheckIntervalMs = LogManager.ProducerIdExpirationCheckIntervalMs,
            scheduler = scheduler,
            time = time,
            brokerTopicStats = brokerTopicStats,
            logDirFailureChannel = logDirFailureChannel)

          logs.put(topicPartition, log)

          info("Created log for partition [%s,%d] in %s with properties {%s}."
            .format(topicPartition.topic,
              topicPartition.partition,
              logDir,
              config.originals.asScala.mkString(", ")))
          // Remove the preferred log dir since it has already been satisfied
          preferredLogDirs.remove(topicPartition)

          log
        } catch {
          case e: IOException =>
            val msg = s"Error while creating log for $topicPartition in dir ${logDir}"
            logDirFailureChannel.maybeAddOfflineLogDir(logDir, msg, e)
            throw new KafkaStorageException(msg, e)
        }
      }
    }
  }

  /**
   *  Delete logs marked for deletion.
   */
  private def deleteLogs(): Unit = {
    try {
      while (!logsToBeDeleted.isEmpty) {
        val removedLog = logsToBeDeleted.take()
        if (removedLog != null) {
          try {
            removedLog.delete()
            info(s"Deleted log for partition ${removedLog.topicPartition} in ${removedLog.dir.getAbsolutePath}.")
          } catch {
            case e: KafkaStorageException =>
              error(s"Exception while deleting $removedLog in dir ${removedLog.dir.getParent}.", e)
          }
        }
      }
    } catch {
      case e: Throwable =>
        error(s"Exception in kafka-delete-logs thread.", e)
    }
  }

  /**
    * Rename the directory of the given topic-partition "logdir" as "logdir.uuid.delete" and
    * add it in the queue for deletion.
    *
    * @param topicPartition TopicPartition that needs to be deleted
    * @return the removed log
    */
  def asyncDelete(topicPartition: TopicPartition): Log = {
    val removedLog: Log = logCreationOrDeletionLock synchronized {
      logs.remove(topicPartition)
    }
    if (removedLog != null) {
      try {
        //We need to wait until there is no more cleaning task on the log to be deleted before actually deleting it.
        if (cleaner != null) {
          cleaner.abortCleaning(topicPartition)
          cleaner.updateCheckpoints(removedLog.dir.getParentFile)
        }
        val dirName = Log.logDeleteDirName(removedLog.name)
        removedLog.close()
        val renamedDir = new File(removedLog.dir.getParent, dirName)
        val renameSuccessful = removedLog.dir.renameTo(renamedDir)
        if (renameSuccessful) {
          checkpointLogStartOffsetsInDir(removedLog.dir.getParentFile)
          removedLog.dir = renamedDir
          // change the file pointers for log and index file
          removedLog.logSegments.foreach(_.updateDir(renamedDir))
          logsToBeDeleted.add(removedLog)
          removedLog.removeLogMetrics()
          info(s"Log for partition ${removedLog.topicPartition} is renamed to ${removedLog.dir.getAbsolutePath} and is scheduled for deletion")
        } else {
          throw new IOException("Failed to rename log directory from " + removedLog.dir.getAbsolutePath + " to " + renamedDir.getAbsolutePath)
        }
      } catch {
        case e: IOException =>
          val msg = s"Error while deleting $topicPartition in dir ${removedLog.dir.getParent}."
          logDirFailureChannel.maybeAddOfflineLogDir(removedLog.dir.getParent, msg, e)
          throw new KafkaStorageException(msg, e)
      }
    } else if (offlineLogDirs.nonEmpty) {
      throw new KafkaStorageException("Failed to delete log for " + topicPartition + " because it may be in one of the offline directories " + offlineLogDirs.mkString(","))
    }
    removedLog
  }

  /**
   * Choose the next directory in which to create a log. Currently this is done
   * by calculating the number of partitions in each directory and then choosing the
   * data directory with the fewest partitions.
   */
  private def nextLogDir(): File = {
    if(_liveLogDirs.size == 1) {
      _liveLogDirs.peek()
    } else {
      // count the number of logs in each parent directory (including 0 for empty directories
      val logCounts = allLogs.groupBy(_.dir.getParent).mapValues(_.size)
      val zeros = _liveLogDirs.asScala.map(dir => (dir.getPath, 0)).toMap
      val dirCounts = (zeros ++ logCounts).toBuffer

      // choose the directory with the least logs in it
      val leastLoaded = dirCounts.sortBy(_._2).head
      new File(leastLoaded._1)
    }
  }

  /**
   * Delete any eligible logs. Return the number of segments deleted.
   * Only consider logs that are not compacted.
   */
  def cleanupLogs() {
    debug("Beginning log cleanup...")
    var total = 0
    val startMs = time.milliseconds
    for(log <- allLogs; if !log.config.compact) {
      debug("Garbage collecting '" + log.name + "'")
      total += log.deleteOldSegments()
    }
    debug("Log cleanup completed. " + total + " files deleted in " +
                  (time.milliseconds - startMs) / 1000 + " seconds")
  }

  /**
   * Get all the partition logs
   */
  def allLogs: Iterable[Log] = logs.values

  /**
   * Get a map of TopicPartition => Log
   */
  def logsByTopicPartition: Map[TopicPartition, Log] = logs.toMap

  /**
   * Map of log dir to logs by topic and partitions in that dir
   */
  private def logsByDir: Map[String, Map[TopicPartition, Log]] = {
    this.logsByTopicPartition.groupBy { case (_, log) => log.dir.getParent }
  }

  // logDir should be an absolute path
  def isLogDirOnline(logDir: String): Boolean = {
    // The logDir should be an absolute path
    if (!logDirs.exists(_.getAbsolutePath == logDir))
      throw new LogDirNotFoundException(s"Log dir $logDir is not found in the config.")

    _liveLogDirs.contains(new File(logDir))
  }

  /**
   * Flush any log which has exceeded its flush interval and has unwritten messages.
   */
  private def flushDirtyLogs(): Unit = {
    debug("Checking for dirty logs to flush...")

    for ((topicPartition, log) <- logs) {
      try {
        val timeSinceLastFlush = time.milliseconds - log.lastFlushTime
        debug("Checking if flush is needed on " + topicPartition.topic + " flush interval  " + log.config.flushMs +
              " last flushed " + log.lastFlushTime + " time since last flush: " + timeSinceLastFlush)
        if(timeSinceLastFlush >= log.config.flushMs)
          log.flush
      } catch {
        case e: Throwable =>
          error("Error flushing topic " + topicPartition.topic, e)
      }
    }
  }
}

object LogManager {

  val RecoveryPointCheckpointFile = "recovery-point-offset-checkpoint"
  val LogStartOffsetCheckpointFile = "log-start-offset-checkpoint"
  val ProducerIdExpirationCheckIntervalMs = 10 * 60 * 1000

  def apply(config: KafkaConfig,
            initialOfflineDirs: Seq[String],
            zkUtils: ZkUtils,
            brokerState: BrokerState,
            kafkaScheduler: KafkaScheduler,
            time: Time,
            brokerTopicStats: BrokerTopicStats,
            logDirFailureChannel: LogDirFailureChannel): LogManager = {
    //将与log相关的配置想拷贝到map，作为默认Log配置
    val defaultProps = KafkaServer.copyKafkaConfigToLog(config)
    //将与log相关的配置项赋值给LogConfig中的各个参数
    val defaultLogConfig = LogConfig(defaultProps)

    //每个 topic 的日志配置信息的映射，包括日志的保留策略、清理策略等。这些配置信息可以从 ZooKeeper 中获取
    val topicConfigs = AdminUtils.fetchAllTopicConfigs(zkUtils).map { case (topic, configs) =>
      topic -> LogConfig.fromProps(defaultProps, configs)
    }

    // read the log configurations from zookeeper
    val cleanerConfig = CleanerConfig(numThreads = config.logCleanerThreads, //日志清理线程数
      //去重缓冲区大小，默认4M
      dedupeBufferSize = config.logCleanerDedupeBufferSize,
      //最大内存可用占比，默认0.9
      dedupeBufferLoadFactor = config.logCleanerDedupeBufferLoadFactor,
      //I/O 缓冲区大小，默认1M
      ioBufferSize = config.logCleanerIoBufferSize,
      //日志中消息最大大小，默认32M
      maxMessageSize = config.messageMaxBytes,
      //清理线程最大读写io速率
      maxIoBytesPerSecond = config.logCleanerIoMaxBytesPerSecond,
      //没有日志清理时的sleep间隔时间
      backOffMs = config.logCleanerBackoffMs,
      //是否开启日志清理，默认true
      enableCleaner = config.logCleanerEnable)

    new LogManager(logDirs = config.logDirs.map(new File(_).getAbsoluteFile), //日志目录列表
      initialOfflineDirs = initialOfflineDirs.map(new File(_).getAbsoluteFile),
      topicConfigs = topicConfigs,
      //这是默认的日志配置，当特定 topic 没有特定的配置时使用。它包含了日志的分段大小、刷新间隔等默认设置
      defaultConfig = defaultLogConfig,
      //这是日志清理器（LogCleaner）的配置，包括清理线程数、去重缓冲区大小、I/O 缓冲区大小等。LogCleaner 负责执行日志压缩，删除过期或无用的日志数据
      cleanerConfig = cleanerConfig,
      //每个日志目录分配的执行加载任务的线程数目。这些线程负责处理日志的读写操作
      ioThreads = config.numRecoveryThreadsPerDataDir,
      //这是触发日志刷新到磁盘的检查间隔时间。Kafka 会根据这个时间间隔定期检查需要刷新到磁盘的日志
      flushCheckMs = config.logFlushSchedulerIntervalMs,
      //这是刷新恢复点检查点的间隔时间。Kafka 会在这个时间间隔内更新每个日志目录下的恢复点检查点文件，该文件记录了每个日志的最大已刷盘偏移量
      flushRecoveryOffsetCheckpointMs = config.logFlushOffsetCheckpointIntervalMs,
      flushStartOffsetCheckpointMs = config.logFlushStartOffsetCheckpointIntervalMs,
      //这是检查日志保留时间的间隔时间。Kafka 会定期检查日志文件是否超过了保留时间，如果超过则进行清理
      retentionCheckMs = config.logCleanupIntervalMs,
      maxPidExpirationMs = config.transactionIdExpirationMs,
      //定时任务调度器，用于安排 LogManager 的各种定时任务，如日志清理、刷新等
      scheduler = kafkaScheduler,
      //当前 broker 的状态，LogManager 会根据 broker 的状态来进行相应的日志管理操作
      brokerState = brokerState,
      //维护了 broker 中 topic 的统计信息，可能会在日志管理过程中用到
      brokerTopicStats = brokerTopicStats,
      logDirFailureChannel = logDirFailureChannel,
      //用于提供当前时间的 Time 对象，它可能会被用于日志的清理和刷新操作中
      time = time)
  }
}
