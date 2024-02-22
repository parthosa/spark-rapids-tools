/*
 * Copyright (c) 2021-2024, NVIDIA CORPORATION.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.rapids.tool.profiling

import java.util.concurrent.TimeUnit

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
import scala.util.control.NonFatal

import com.nvidia.spark.rapids.tool.profiling._

import org.apache.spark.TaskFailedReason
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler._
import org.apache.spark.sql.execution.ui.{SparkListenerSQLAdaptiveExecutionUpdate, SparkListenerSQLAdaptiveSQLMetricUpdates, SparkListenerSQLExecutionStart}
import org.apache.spark.sql.rapids.tool.EventProcessorBase
import org.apache.spark.sql.rapids.tool.util.{EventUtils, StringUtils}

/**
 * This class is to process all events and do validation in the end.
 */
class EventsProcessor(app: ApplicationInfo) extends EventProcessorBase[ApplicationInfo](app)
  with Logging {

  override def doSparkListenerJobStart(
      app: ApplicationInfo,
      event: SparkListenerJobStart): Unit = {
    logDebug("Processing event: " + event.getClass)
    super.doSparkListenerJobStart(app, event)
    val sqlIDString = event.properties.getProperty("spark.sql.execution.id")
    val sqlID = StringUtils.stringToLong(sqlIDString)
    // add jobInfoClass
    val thisJob = new JobInfoClass(
      event.jobId,
      event.stageIds,
      sqlID,
      event.properties.asScala,
      event.time,
      None,
      None,
      None,
      None,
      app.isGPUModeEnabledForJob(event)
    )
    app.jobIdToInfo.put(event.jobId, thisJob)
  }

  override def doSparkListenerResourceProfileAddedReflect(
      app: ApplicationInfo,
      event: SparkListenerEvent): Boolean = {
    val rpAddedClass = "org.apache.spark.scheduler.SparkListenerResourceProfileAdded"
    if (event.getClass.getName.equals(rpAddedClass)) {
      try {
        event match {
          case _: SparkListenerResourceProfileAdded =>
            doSparkListenerResourceProfileAdded(app,
              event.asInstanceOf[SparkListenerResourceProfileAdded])
            true
          case _ => false
        }
      } catch {
        case _: ClassNotFoundException =>
          logWarning("Error trying to parse SparkListenerResourceProfileAdded, Spark" +
            " version likely older than 3.1.X, unable to parse it properly.")
          false
      }
    } else {
      false
    }
  }

  override def doSparkListenerResourceProfileAdded(
      app: ApplicationInfo,
      event: SparkListenerResourceProfileAdded): Unit = {

    logDebug("Processing event: " + event.getClass)
    // leave off maxTasks for now
    val rp = ResourceProfileInfoCase(event.resourceProfile.id,
      event.resourceProfile.executorResources, event.resourceProfile.taskResources)
    app.resourceProfIdToInfo(event.resourceProfile.id) = rp
  }

  override def doSparkListenerBlockManagerRemoved(
      app: ApplicationInfo,
      event: SparkListenerBlockManagerRemoved): Unit = {
    logDebug("Processing event: " + event.getClass)
    val thisBlockManagerRemoved = BlockManagerRemovedCase(
      event.blockManagerId.executorId,
      event.blockManagerId.host,
      event.blockManagerId.port,
      event.time
    )
    app.blockManagersRemoved += thisBlockManagerRemoved
  }

  override def doSparkListenerEnvironmentUpdate(
      app: ApplicationInfo,
      event: SparkListenerEnvironmentUpdate): Unit = {
    logDebug("Processing event: " + event.getClass)
    super.doSparkListenerEnvironmentUpdate(app, event)

    logDebug(s"App's GPU Mode = ${app.gpuMode}")
  }

  override def doSparkListenerApplicationStart(
      app: ApplicationInfo,
      event: SparkListenerApplicationStart): Unit = {
    logDebug("Processing event: " + event.getClass)
    val thisAppStart = ApplicationCase(
      event.appName,
      event.appId,
      event.sparkUser,
      event.time,
      None,
      None,
      "",
      "",
      pluginEnabled = false
    )
    app.appInfo = thisAppStart
    app.appId = event.appId.getOrElse("")
  }

  override def doSparkListenerApplicationEnd(
      app: ApplicationInfo,
      event: SparkListenerApplicationEnd): Unit = {
    logDebug("Processing event: " + event.getClass)
    app.appEndTime = Some(event.time)
  }

  override def doSparkListenerTaskStart(
      app: ApplicationInfo,
      event: SparkListenerTaskStart): Unit = {
    logDebug("Processing event: " + event.getClass)
    // currently not used
    // app.taskStart += event
  }

  override def doSparkListenerTaskEnd(
      app: ApplicationInfo,
      event: SparkListenerTaskEnd): Unit = {
    logDebug("Processing event: " + event.getClass)
    super.doSparkListenerTaskEnd(app, event)
    val reason = event.reason match {
      case failed: TaskFailedReason =>
        failed.toErrorString
      case _ =>
        event.reason.toString
    }

    val thisTask = TaskCase(
      event.stageId,
      event.stageAttemptId,
      event.taskType,
      reason,
      event.taskInfo.taskId,
      event.taskInfo.attemptNumber,
      event.taskInfo.launchTime,
      event.taskInfo.finishTime,
      event.taskInfo.duration,
      event.taskInfo.successful,
      event.taskInfo.executorId,
      event.taskInfo.host,
      event.taskInfo.taskLocality.toString,
      event.taskInfo.speculative,
      event.taskInfo.gettingResultTime,
      event.taskMetrics.executorDeserializeTime,
      TimeUnit.NANOSECONDS.toMillis(event.taskMetrics.executorDeserializeCpuTime),
      event.taskMetrics.executorRunTime,
      TimeUnit.NANOSECONDS.toMillis(event.taskMetrics.executorCpuTime),
      event.taskMetrics.peakExecutionMemory,
      event.taskMetrics.resultSize,
      event.taskMetrics.jvmGCTime,
      event.taskMetrics.resultSerializationTime,
      event.taskMetrics.memoryBytesSpilled,
      event.taskMetrics.diskBytesSpilled,
      event.taskMetrics.shuffleReadMetrics.remoteBlocksFetched,
      event.taskMetrics.shuffleReadMetrics.localBlocksFetched,
      event.taskMetrics.shuffleReadMetrics.fetchWaitTime,
      event.taskMetrics.shuffleReadMetrics.remoteBytesRead,
      event.taskMetrics.shuffleReadMetrics.remoteBytesReadToDisk,
      event.taskMetrics.shuffleReadMetrics.localBytesRead,
      event.taskMetrics.shuffleReadMetrics.totalBytesRead,
      event.taskMetrics.shuffleWriteMetrics.bytesWritten,
      TimeUnit.NANOSECONDS.toMillis(event.taskMetrics.shuffleWriteMetrics.writeTime),
      event.taskMetrics.shuffleWriteMetrics.recordsWritten,
      event.taskMetrics.inputMetrics.bytesRead,
      event.taskMetrics.inputMetrics.recordsRead,
      event.taskMetrics.outputMetrics.bytesWritten,
      event.taskMetrics.outputMetrics.recordsWritten
    )
    app.taskEnd += thisTask
  }

  override def doSparkListenerSQLExecutionStart(
      app: ApplicationInfo,
      event: SparkListenerSQLExecutionStart): Unit = {
    super.doSparkListenerSQLExecutionStart(app, event)
    app.physicalPlanDescription += (event.executionId -> event.physicalPlanDescription)
  }

  override def doSparkListenerStageCompleted(
      app: ApplicationInfo,
      event: SparkListenerStageCompleted): Unit = {
    logDebug("Processing event: " + event.getClass)
    super.doSparkListenerStageCompleted(app, event)

    // Parse stage accumulables
    for (res <- event.stageInfo.accumulables) {
      try {
        val accumInfo = res._2
        EventUtils.buildTaskStageAccumFromAccumInfo(accumInfo,
          event.stageInfo.stageId, event.stageInfo.attemptNumber()).foreach { thisMetric =>
          val arrBuf = app.taskStageAccumMap.getOrElseUpdate(accumInfo.id,
            ArrayBuffer[TaskStageAccumCase]())
          app.accumIdToStageId.put(accumInfo.id, event.stageInfo.stageId)
          arrBuf += thisMetric
        }
      } catch {
        case NonFatal(e) =>
          logWarning("Exception when parsing accumulables on stage-completed " +
              "stageID=" + event.stageInfo.stageId + ": ")
          logWarning(e.toString)
          logWarning("The problematic accumulable is: name="
              + res._2.name + ",value=" + res._2.value + ",update=" + res._2.update)
      }
    }
  }

  override def doSparkListenerTaskGettingResult(
      app: ApplicationInfo,
      event: SparkListenerTaskGettingResult): Unit = {
    logDebug("Processing event: " + event.getClass)
  }

  override def doSparkListenerSQLAdaptiveExecutionUpdate(
      app: ApplicationInfo,
      event: SparkListenerSQLAdaptiveExecutionUpdate): Unit = {
    logDebug("Processing event: " + event.getClass)
    // AQE plan can override the ones got from SparkListenerSQLExecutionStart
    app.physicalPlanDescription += (event.executionId -> event.physicalPlanDescription)
    super.doSparkListenerSQLAdaptiveExecutionUpdate(app, event)
  }

  override def doSparkListenerSQLAdaptiveSQLMetricUpdates(
      app: ApplicationInfo,
      event: SparkListenerSQLAdaptiveSQLMetricUpdates): Unit = {
    logDebug("Processing event: " + event.getClass)
    val SparkListenerSQLAdaptiveSQLMetricUpdates(sqlID, sqlPlanMetrics) = event
    val metrics = sqlPlanMetrics.map { metric =>
      SQLPlanMetricsCase(sqlID, metric.name,
        metric.accumulatorId, metric.metricType)
    }
    app.sqlPlanMetricsAdaptive ++= metrics
  }

  // To process all other unknown events
  override def doOtherEvent(app: ApplicationInfo, event: SparkListenerEvent): Unit = {
    logDebug("Skipping unhandled event: " + event.getClass)
    // not used
  }
}
