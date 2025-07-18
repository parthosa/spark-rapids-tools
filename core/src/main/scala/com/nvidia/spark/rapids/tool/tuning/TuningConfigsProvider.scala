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

import java.util

import scala.beans.BeanProperty
import scala.collection.JavaConverters._

import org.apache.spark.network.util.ByteUnit
import org.apache.spark.sql.rapids.tool.util.{StringUtils, ValidatableProperties}

/**
 * Represents a tuning configuration entry with name, description, default, min, and max values.
 */
class TuningConfigEntry(
  @BeanProperty var name: String,
  @BeanProperty var description: String,
  @BeanProperty var default: String,
  @BeanProperty var min: String,
  @BeanProperty var max: String) extends ValidatableProperties {

  def this() = this("", "", "", "", "")

  private def isEmptyValue(value: String): Boolean = {
    value == null || value.isEmpty
  }

  private def isEmpty: Boolean = {
    isEmptyValue(name) && isEmptyValue(description) &&
      isEmptyValue(default) && isEmptyValue(min) && isEmptyValue(max)
  }

  // Helper methods for memory unit conversions
  // These methods return the memory values converted to the specified ByteUnit
  // as Long values.
  def getDefaultAsMemory(unit: ByteUnit): Long = {
    getMemoryValueInUnit(default, unit)
  }

  def getMinAsMemory(unit: ByteUnit): Long = {
    getMemoryValueInUnit(min, unit)
  }

  def getMaxAsMemory(unit: ByteUnit): Long = {
    getMemoryValueInUnit(max, unit)
  }

  /** Helper method to convert memory string to target unit */
  private def getMemoryValueInUnit(value: String, targetUnit: ByteUnit): Long = {
    val valueInBytes = StringUtils.convertMemorySizeToBytes(value, Some(ByteUnit.BYTE))
    ByteUnit.BYTE.convertTo(valueInBytes, targetUnit)
  }

  /**
   * Merge this TuningConfigEntry with another one. The other entry's values will override
   * this entry's values if they are not empty.
   * @param other The other TuningConfigEntry to merge with
   * @return A new TuningConfigEntry with merged values
   */
  def merge(other: TuningConfigEntry): TuningConfigEntry = {
    require(this.name == other.name, s"Cannot merge configs with different names: '" +
      s"${this.name}' vs '${other.name}'")

    new TuningConfigEntry(
      name = this.name,
      description = if (isEmptyValue(other.description)) this.description else other.description,
      default = if (isEmptyValue(other.default)) this.default else other.default,
      min = if (isEmptyValue(other.min)) this.min else other.min,
      max = if (isEmptyValue(other.max)) this.max else other.max
    )
  }

  override def toString: String = {
    s"TuningConfigEntry(name='$name', description='$description'," +
      s" default='$default', min='$min', max='$max')"
  }

  override def validate(): Unit = {
    // Skip validation if the entry is completely empty (i.e. when loaded from YAML with no values)
    if (!isEmpty) {
      // Validate the name and description are not empty
      if (isEmptyValue(name)) {
        throw new IllegalArgumentException(s"Name must be defined for config '$name'")
      }
      // Validate at least one of the default, min, max is defined
      if (isEmptyValue(default) && isEmptyValue(min) && isEmptyValue(max)) {
        throw new IllegalArgumentException(s"At least one of the default, min, max " +
          s"must be defined for config '$name'")
      }
    }
  }
}

/**
 * Represents the tuning configurations loaded from the tuning table YAML file.
 * This class provides dynamic access to configs with description, default, min, and max values.
 *
 * Example usage:
 * {{{
 *   val provider = TuningConfigsProvider(defaultConfigs)
 *   provider.withTool(QualificationAutoTuner)
 *
 *   // Get configuration values
 *   val heapPerCore = provider.get("HEAP_PER_CORE_MB").getDefault[Long]()
 *   val maxPinned = provider.get("PINNED_MEMORY_MB").getMax[Long]()
 * }}}
 */
class TuningConfigsProvider (
    @BeanProperty var default: util.List[TuningConfigEntry],
    @BeanProperty var qualification: util.List[TuningConfigEntry],
    @BeanProperty var profiling: util.List[TuningConfigEntry]) extends ValidatableProperties {

  def this() = this(
    new util.ArrayList[TuningConfigEntry](),
    new util.ArrayList[TuningConfigEntry](),
    new util.ArrayList[TuningConfigEntry]())

  private var selectedTool: Option[AutoTuner] = None

  /** Tool-specific overrides for qualification/profiling */
  private lazy val toolOverrides: util.List[TuningConfigEntry] = selectedTool match {
    case Some(_: QualificationAutoTuner) => qualification
    case Some(_: ProfilingAutoTuner) => profiling
    case _ => throw new IllegalArgumentException(
      "Tool must be specified as either QualificationAutoTuner or ProfilingAutoTuner")
  }

  /** Cached lookup map, rebuilt from default and toolOverrides */
  @transient
  private lazy val tuningConfigsMap: Map[String, TuningConfigEntry] = {
    val mergedConfigs = mergeConfigs(default, toolOverrides)
    mergedConfigs.asScala.map(e => e.name -> e).toMap
  }

  /**
   * Merges entries from the override list into the base list.
   * Returns a new list containing the merged entries.
   */
  private def mergeConfigs(
      baseList: util.List[TuningConfigEntry],
      overrideList: util.List[TuningConfigEntry])
  : util.List[TuningConfigEntry] = {
    if (overrideList == null || overrideList.isEmpty) {
      return baseList
    }

    val result = new util.ArrayList[TuningConfigEntry](baseList)
    val baseMap = baseList.asScala.map(e => e.name -> e).toMap

    overrideList.asScala.foreach { overrideEntry =>
      baseMap.get(overrideEntry.name) match {
        case Some(baseEntry) =>
          // Entry exists in base list, merge and update
          val index = result.indexOf(baseEntry)
          result.set(index, baseEntry.merge(overrideEntry))
        case None =>
          // Entry does not exist in base list, add it
          result.add(overrideEntry)
      }
    }
    result
  }

  def withTool(maybeTuner: Option[AutoTuner]): TuningConfigsProvider = {
    this.selectedTool = maybeTuner
    this
  }

  /**
   * Get a config entry by name.
   * @param key The config name (e.g., "HEAP_PER_CORE_MB")
   * @return The config entry
   */
  def getEntry(key: String): TuningConfigEntry = {
    tuningConfigsMap(key)
  }

  /**
   * Merge another TuningConfigsProvider instance into this one.
   * Returns a new instance with merged configurations.
   */
  def merge(other: TuningConfigsProvider): TuningConfigsProvider = {
    new TuningConfigsProvider(
      mergeConfigs(this.default, other.default),
      mergeConfigs(this.qualification, other.qualification),
      mergeConfigs(this.profiling, other.profiling)
    ).withTool(this.selectedTool)
  }

  override def validate(): Unit = {
    default.asScala.foreach(_.validate())
    qualification.asScala.foreach(_.validate())
    profiling.asScala.foreach(_.validate())
  }
}

object TuningConfigsProvider {
  val DEFAULT_CONFIGS_FILE = "bootstrap/tuningConfigs.yaml"

  def apply(
      default: util.List[TuningConfigEntry] = new util.ArrayList[TuningConfigEntry](),
      qualification: util.List[TuningConfigEntry] = new util.ArrayList[TuningConfigEntry](),
      profiling: util.List[TuningConfigEntry] = new util.ArrayList[TuningConfigEntry]()
  ): TuningConfigsProvider = {
    new TuningConfigsProvider(default, qualification, profiling)
  }
}
