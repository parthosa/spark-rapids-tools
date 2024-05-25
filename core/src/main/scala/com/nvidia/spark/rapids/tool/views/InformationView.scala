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

import com.nvidia.spark.rapids.tool.analysis.{ProfAppIndexMapperTrait, QualAppIndexMapperTrait}
import com.nvidia.spark.rapids.tool.profiling.AppInfoProfileResults

import org.apache.spark.sql.rapids.tool.AppBase


trait AppInformationViewTrait extends ViewableTrait[AppInfoProfileResults] {
  override def getLabel: String = "Application Information"

  def getRawView(app: AppBase, index: Int): Seq[AppInfoProfileResults] = {
    app.appMetaData.map { a =>
      AppInfoProfileResults(index, a.appName, a.appId,
        a.sparkUser, a.startTime, a.endTime, app.getAppDuration,
        a.getDurationString, app.sparkVersion, app.gpuMode)
    }.toSeq
  }
  override def sortView(rows: Seq[AppInfoProfileResults]): Seq[AppInfoProfileResults] = {
    rows.sortBy(cols => cols.appIndex)
  }
}


object QualInformationView extends AppInformationViewTrait with QualAppIndexMapperTrait {
  // Keep for the following refactor stages to customize the view based on the app type (Qual/Prof)
}

object ProfInformationView extends AppInformationViewTrait with ProfAppIndexMapperTrait {
  // Keep for the following refactor stages to customize the view based on the app type (Qual/Prof)
}
