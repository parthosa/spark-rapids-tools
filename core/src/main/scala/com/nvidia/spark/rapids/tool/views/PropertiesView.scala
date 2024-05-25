/*
 * Copyright (c) 2024, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids.tool.views

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import com.nvidia.spark.rapids.tool.analysis.{ProfAppIndexMapperTrait, QualAppIndexMapperTrait}
import com.nvidia.spark.rapids.tool.profiling.RapidsPropertyProfileResult

import org.apache.spark.sql.rapids.tool.{AppBase, ToolUtils}


/**
 * Print RAPIDS related or all Spark Properties when the propSource is set to "rapids".
 * Note that RAPIDS related properties are not necessarily starting with prefix 'spark.rapids'.
 * This table is inverse of the other tables where the row keys are property keys and the
 columns
 * are the application values. So column1 would be all the key values for app index 1.
 * @return List of properties relevant to the source.
 */
trait AppPropertiesViewTrait extends ViewableTrait[RapidsPropertyProfileResult] {

  val propSource: String

  private val labelMap: Map[String, String] = Map(
    "rapids" -> "Spark Rapids parameters set explicitly",
    "spark" -> "Spark Properties",
    "system" -> "System Properties"
  )
  override def getLabel: String = labelMap.getOrElse(propSource, "Unknown")

  val outputHeaders: ArrayBuffer[String] = ArrayBuffer("propertyName")
  val props: mutable.HashMap[String, ArrayBuffer[String]] =
    mutable.HashMap[String, ArrayBuffer[String]]()

  def addNewProps(newRapidsRelated: Map[String, String]): Unit = {
    val inter = props.keys.toSeq.intersect(newRapidsRelated.keys.toSeq)
    val existDiff = props.keys.toSeq.diff(inter)
    val newDiff = newRapidsRelated.keys.toSeq.diff(inter)

    inter.foreach { key =>
      props(key) += newRapidsRelated.getOrElse(key, "null")
    }

    existDiff.foreach { key =>
      props(key) += "null"
    }

    newDiff.foreach { key =>
      val appVals = ArrayBuffer.fill(0)("null") += newRapidsRelated.getOrElse(key, "null")
      props.put(key, appVals)
    }
  }

  def getRawView(app: AppBase, index: Int): Seq[RapidsPropertyProfileResult] = {
    outputHeaders += s"appIndex_$index"

    val propsToKeep = propSource match {
      case "rapids" =>
        app.sparkProperties.filterKeys(ToolUtils.isRapidsPropKey)
      case "spark" =>
        app.sparkProperties.filterKeys(key => !key.contains(ToolUtils.PROPS_RAPIDS_KEY_PREFIX))
      case _ =>
        app.systemProperties
    }

    addNewProps(propsToKeep)
    val allRows = props.map { case (k, v) => Seq(k) ++ v }.toSeq
    val resRows = allRows.map(r => RapidsPropertyProfileResult(r.head, outputHeaders, r))
    resRows
  }

  override def sortView(rows: Seq[RapidsPropertyProfileResult])
  : Seq[RapidsPropertyProfileResult] = {
    rows.sortBy(cols => cols.key)
  }
}

case class QualPropertiesView(propSource: String)
  extends AppPropertiesViewTrait with QualAppIndexMapperTrait {
  // Keep for the following refactor stages to customize the view based on the app type (Qual/Prof)
}

case class ProfPropertiesView(propSource: String)
  extends AppPropertiesViewTrait with ProfAppIndexMapperTrait {
  // Keep for the following refactor stages to customize the view based on the app type (Qual/Prof)
}
