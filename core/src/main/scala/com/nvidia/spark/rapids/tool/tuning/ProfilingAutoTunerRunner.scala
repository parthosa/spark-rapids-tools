/*
 * Copyright (c) 2025, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids.tool.tuning

import java.nio.file.Paths

import scala.util.{Failure, Success, Try}

import com.nvidia.spark.rapids.tool.{AppSummaryInfoBaseProvider, Platform, ToolTextFileWriter}
import com.nvidia.spark.rapids.tool.profiling.{BaseDriverLogInfoProvider, DriverLogInfoProvider, ProfilerResult, RecommendedCommentResult}
import org.apache.hadoop.conf.Configuration

import org.apache.spark.internal.Logging

/**
 * A wrapper class to run the AutoTuner for Profiling Tool.
 * @param appInfoProvider Provider of the profiling analysis data
 * @param tunerContext Container which holds the arguments passed to the AutoTuner execution
 * @param driverInfoProvider Entity that implements APIs needed to extract information from the
 *                           driver log if any
 */
class ProfilingAutoTunerRunner(val appInfoProvider: AppSummaryInfoBaseProvider,
    val tunerContext: TunerContext,
    val driverInfoProvider: DriverLogInfoProvider) {

  private def writeTuningReport(recommendations: Seq[TuningEntryTrait],
      comments: Seq[RecommendedCommentResult],
      appId: String, outputDir: String, hadoopConf: Configuration): Unit = {
    // Write down the recommendations and the comments
    val finalOutputDir = Paths.get(outputDir, appId).toString
    val textFileWriter = new ToolTextFileWriter(finalOutputDir,
      "tuning.log", s"Profiling AutoTuner - $appId", Option(hadoopConf))
    try {
      textFileWriter.write(
        s"### Recommended SPARK Configuration on GPU Cluster for App: $appId ###\n")
      textFileWriter.write(getAutoTunerResultsAsString(recommendations, comments))
    } finally {
      textFileWriter.close()
    }
  }

  def runAutoTuner(platform: Platform,
                   userProvidedTuningConfigs: Option[TuningConfigsProvider]):
                   (Seq[TuningEntryTrait], Seq[RecommendedCommentResult]) = {
    val autoTuner: AutoTuner =
      ProfilingAutoTunerHelper.buildAutoTuner(appInfoProvider, platform,
        driverInfoProvider, userProvidedTuningConfigs)
    val (recommendations, comments) = autoTuner.getRecommendedProperties()

    // Get app ID for output file naming
    val appId = appInfoProvider.getAppID
    writeTuningReport(recommendations, comments, appId,
      tunerContext.getOutputPath, tunerContext.hadoopConf)

    (recommendations, comments)
  }

  private def getAutoTunerResultsAsString(props: Seq[TuningEntryTrait],
      comments: Seq[RecommendedCommentResult]): String = {
    val propStr = if (props.nonEmpty) {
        val propertiesToStr = props.map(_.toConfString).reduce(_ + "\n" + _)
        s"\nSpark Properties:\n$propertiesToStr\n"
      } else {
        "Cannot recommend properties. See Comments.\n"
      }
    if (comments.isEmpty) { // Comments are optional
      propStr
    } else {
      val commentsToStr = comments.map(_.toString).reduce(_ + "\n" + _)
      propStr + s"\nComments:\n$commentsToStr\n"
    }
  }
}

object ProfilingAutoTunerRunner extends Logging {

  def apply(profilerResult: Option[ProfilerResult],
      tunerContext: TunerContext,
      driverInfoProvider: DriverLogInfoProvider = BaseDriverLogInfoProvider.noneDriverLog):
      Option[ProfilingAutoTunerRunner] = {
    Try {
      val appInfoProvider = AppSummaryInfoBaseProvider.fromAppInfo(profilerResult)
      new ProfilingAutoTunerRunner(appInfoProvider, tunerContext, driverInfoProvider)
    } match {
      case Success(runner) => Some(runner)
      case Failure(e) =>
        val appId = profilerResult.flatMap(_.summary.appInfo.headOption.map(_.appId))
          .getOrElse("unknown")
        logError(s"Failed to create Profiling tuning object for application $appId", e)
        None
    }
  }
}
