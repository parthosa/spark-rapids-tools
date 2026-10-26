/*
 * Copyright (c) 2026, NVIDIA CORPORATION.
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

import scala.collection.mutable

import com.nvidia.spark.rapids.tool._
import com.nvidia.spark.rapids.tool.profiling.BaseDriverLogInfoProvider
import com.nvidia.spark.rapids.tool.tuning.config.TuningConfiguration
import org.scalatest.matchers.should.Matchers._

import org.apache.spark.sql.rapids.tool.util.PropertiesLoader

class ClusterSizingStrategySuite extends BaseAutoTunerSuite {
  override val autoTunerHelper: AutoTunerHelper = QualificationAutoTunerHelper

  private val helpers = Seq(
    "qualification" -> QualificationAutoTunerHelper,
    "profiling" -> ProfilingAutoTunerHelper)
  private val strategies = Seq(
    "SourceCoresPreservingStrategy" -> SourceCoresPreservingStrategy,
    "ConstantTotalCoresStrategy" -> ConstantTotalCoresStrategy,
    "ConstantGpuCountStrategy" -> ConstantGpuCountStrategy)
  private val platforms = Seq(PlatformNames.ONPREM, PlatformNames.EMR, PlatformNames.DATAPROC,
    PlatformNames.DATAPROC_GKE, PlatformNames.DATABRICKS_AWS, PlatformNames.DATABRICKS_AZURE,
    PlatformNames.DATAPROC_SL)

  for ((tool, helper) <- helpers; platformName <- platforms; withWorker <- Seq(false, true)) {
    test(s"$tool auto sizing on $platformName with target worker=$withWorker") {
      val target = if (withWorker) {
        ToolTestUtils.buildTargetClusterInfo(cpuCores = Some(32), memoryGB = Some(128L),
          gpuCount = Some(1))
      } else {
        ToolTestUtils.buildTargetClusterInfo(
          enforcedSparkProperties = Map("spark.executor.memory" -> "32g"))
      }
      val platform = PlatformFactory.createInstance(platformName, Some(target))
      val expected = if (platformName == PlatformNames.ONPREM && !withWorker) {
        SourceCoresPreservingStrategy
      } else if (tool == "qualification") {
        ConstantTotalCoresStrategy
      } else {
        ConstantGpuCountStrategy
      }
      helper.recommendedClusterSizingStrategy(platform) shouldBe expected
      helper.recommendedClusterSizingStrategy(platform, "auto") shouldBe expected
      strategies.foreach { case (name, strategy) =>
        helper.recommendedClusterSizingStrategy(platform, name) shouldBe strategy
      }
    }
  }

  private def recommend(
      helper: AutoTunerHelper,
      config: Option[TuningConfiguration],
      target: Option[TargetClusterProps] = None): (Int, Int) = {
    val props = mutable.Map(
      "spark.master" -> "yarn",
      "spark.executor.cores" -> "2",
      "spark.executor.instances" -> "2",
      "spark.executor.memory" -> "16g")
    val info = getMockInfoProvider(0, Seq(0), Seq(0.0), props, Some(testSparkVersion))
    val platform = PlatformFactory.createInstance(PlatformNames.ONPREM, target)
    configureEventLogClusterInfoForTest(platform, numCores = 2, numWorkers = 2,
      sparkProperties = props.toMap)
    val tuner = helper.createAutoTunerInstance(info, platform,
      BaseDriverLogInfoProvider.noneDriverLog, config)
    tuner.getRecommendedProperties(showOnlyUpdatedProps = false)
    val cluster = platform.recommendedClusterInfo.get
    (cluster.numExecutors, cluster.coresPerExecutor)
  }

  for ((tool, helper) <- helpers) {
    test(s"$tool preserves two executors with two cores by default") {
      recommend(helper, None) shouldBe ((2, 2))
    }

    for ((strategy, expected) <- Seq(
        "auto" -> ((2, 2)),
        "SourceCoresPreservingStrategy" -> ((2, 2)),
        "ConstantTotalCoresStrategy" -> ((1, 4)),
        "ConstantGpuCountStrategy" -> ((2, 4)))) {
      test(s"$tool uses configured $strategy through tuning config precedence") {
        val defaultConfig = PropertiesLoader[TuningConfiguration].loadFromContent(
          s"""default:
             |  - name: CLUSTER_SIZING_STRATEGY
             |    default: $strategy
             |""".stripMargin)
        recommend(helper, defaultConfig) shouldBe expected

        val toolConfig = PropertiesLoader[TuningConfiguration].loadFromContent(
          s"""default:
             |  - name: CLUSTER_SIZING_STRATEGY
             |    default: ConstantGpuCountStrategy
             |$tool:
             |  - name: CLUSTER_SIZING_STRATEGY
             |    default: $strategy
             |""".stripMargin)
        recommend(helper, toolConfig) shouldBe expected
      }
    }

    test(s"$tool sizing strategies retain enforced and preserved Spark properties") {
      strategies.foreach { case (name, _) =>
        val config = PropertiesLoader[TuningConfiguration].loadFromContent(
          s"""default:
             |  - name: CLUSTER_SIZING_STRATEGY
             |    default: $name
             |""".stripMargin)
        val enforced = ToolTestUtils.buildTargetClusterInfo(enforcedSparkProperties = Map(
          "spark.executor.cores" -> "3", "spark.executor.instances" -> "5"))
        recommend(helper, config, Some(enforced)) shouldBe ((5, 3))
        val preserved = ToolTestUtils.buildTargetClusterInfo(preserveSparkProperties =
          List("spark.executor.cores", "spark.executor.instances"))
        recommend(helper, config, Some(preserved)) shouldBe ((2, 2))
      }
    }
  }

  for ((tool, helper) <- helpers; platformName <- Seq(PlatformNames.EMR, PlatformNames.ONPREM);
      sourceExecutors <- Seq(2, 18)) {
    test(s"$tool preserves source sizing through $platformName worker resolution " +
        s"with $sourceExecutors executors") {
      val config = PropertiesLoader[TuningConfiguration].loadFromContent(
        """default:
          |  - name: CLUSTER_SIZING_STRATEGY
          |    default: SourceCoresPreservingStrategy
          |""".stripMargin)
      for (propertyMode <- Seq("source", "enforced", "preserved", "capacity")) {
        val sourceCores = if (propertyMode == "capacity") 32 else 2
        val props = mutable.Map(
          "spark.master" -> "yarn",
          "spark.executor.cores" -> sourceCores.toString,
          "spark.executor.instances" -> sourceExecutors.toString,
          "spark.executor.memory" -> "16g")
        val enforced = if (propertyMode == "enforced") {
          Map("spark.executor.cores" -> "3", "spark.executor.instances" -> "5")
        } else {
          Map.empty[String, String]
        }
        val preserved = if (propertyMode == "preserved") {
          List("spark.executor.cores", "spark.executor.instances")
        } else {
          List.empty[String]
        }
        val target = ToolTestUtils.buildTargetClusterInfo(
          cpuCores = if (platformName == PlatformNames.ONPREM) Some(16) else None,
          memoryGB = if (platformName == PlatformNames.ONPREM) Some(64L) else None,
          gpuCount = if (platformName == PlatformNames.ONPREM) Some(2) else None,
          gpuDevice = if (platformName == PlatformNames.ONPREM) Some(GpuTypes.L4) else None,
          enforcedSparkProperties = enforced, preserveSparkProperties = preserved)
        val platform = PlatformFactory.createInstance(platformName, Some(target))
        platform.recommendedWorkerNode.isDefined shouldBe true
        val info = getMockInfoProvider(0, Seq(0), Seq(0.0), props, Some(testSparkVersion))
        configureEventLogClusterInfoForTest(platform, numCores = sourceCores,
          numWorkers = sourceExecutors, sparkProperties = props.toMap)
        val tuner = helper.createAutoTunerInstance(info, platform,
          BaseDriverLogInfoProvider.noneDriverLog, config)
        val properties = tuner.getRecommendedProperties(showOnlyUpdatedProps = false)._1
          .map(p => p.name -> p.getTuneValue()).toMap
        val cluster = platform.recommendedClusterInfo.get
        val expectedCores = propertyMode match {
          case "enforced" => 3
          case "capacity" => platform.recommendedCoresPerExec
          case _ => sourceCores
        }
        val expectedExecutors = if (propertyMode == "enforced") 5 else sourceExecutors
        val expectedWorkers = if (platformName == PlatformNames.ONPREM) {
          -1
        } else {
          math.ceil(expectedExecutors.toDouble / cluster.numGpusPerNode).toInt
        }
        withClue(s"$propertyMode: ") {
          cluster.coresPerExecutor shouldBe expectedCores
          cluster.numExecutors shouldBe expectedExecutors
          cluster.numWorkerNodes shouldBe expectedWorkers
          properties("spark.executor.cores") shouldBe expectedCores.toString
          properties("spark.executor.instances") shouldBe expectedExecutors.toString
        }
      }
    }
  }

  for (section <- Seq("default", "qualification", "profiling");
      invalid <- Seq("unknown", "AUTO", "constanttotalcoresstrategy")) {
    test(s"reject invalid sizing strategy '$invalid' in $section") {
      val error = intercept[IllegalArgumentException] {
        PropertiesLoader[TuningConfiguration].loadFromContent(
          s"""$section:
             |  - name: CLUSTER_SIZING_STRATEGY
             |    default: $invalid
             |""".stripMargin)
      }
      error.getMessage should include (s"Invalid CLUSTER_SIZING_STRATEGY '$invalid'")
      strategies.foreach { case (name, _) => error.getMessage should include (name) }
      error.getMessage should include ("auto")
    }
  }
}
