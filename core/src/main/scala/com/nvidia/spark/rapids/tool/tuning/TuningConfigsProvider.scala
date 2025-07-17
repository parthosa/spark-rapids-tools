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
 *   val provider = new TuningConfigsProvider()
 *   provider.setToolName("profiling")
 *
 *   // Get configuration values
 *   val heapPerCore = provider.get("HEAP_PER_CORE_MB").getDefault[Long]()
 *   val maxPinned = provider.get("PINNED_MEMORY_MB").getMax[Long]()
 *
 *   // Get configuration description
 *   val description = provider.getDescription("HEAP_PER_CORE_MB")
 *
 *   // Get all config descriptions
 *   val allDescriptions = provider.getAllConfigDescriptions
 * }}}
 */
class TuningConfigsProvider(
  @BeanProperty var default: java.util.List[TuningConfigEntry],
  @BeanProperty var qualification: java.util.List[TuningConfigEntry],
  @BeanProperty var profiling: java.util.List[TuningConfigEntry]
) extends ValidatableProperties {

  // Used to determine which tool's configs to use
  var autoTunerHelper: Option[AutoTunerHelper] = None

  def this() = this(new java.util.ArrayList[TuningConfigEntry](),
    new java.util.ArrayList[TuningConfigEntry](),
    new java.util.ArrayList[TuningConfigEntry]())

  def setAutoTunerHelper(autoTunerHelper: AutoTunerHelper): Unit = {
    this.autoTunerHelper = Some(autoTunerHelper)
  }

  /**
   * Merges entries from the override list into the base list.
   * Entries are matched by name. If an entry exists in both lists, they are merged.
   * Entries only in the override list are added.
   * NOTE: The base list is modified in place.
   * @param baseList The base list of TuningConfigEntry (modified in place)
   * @param overrideList The overriding list of TuningConfigEntry to merge into baseList
   */
  private def mergeInto(
      baseList: java.util.List[TuningConfigEntry],
      overrideList: java.util.List[TuningConfigEntry]): Unit = {
    if (overrideList == null || overrideList.isEmpty) {
      return
    }
    val baseMap = baseList.asScala.zipWithIndex.map { case (e, i) => e.name -> i }.toMap
    // Loop through override entries and either merge or add them
    overrideList.asScala.foreach { overrideEntry =>
      baseMap.get(overrideEntry.name) match {
        case Some(index) =>
          // Entry exists in base list, merge and update
          val mergedEntry = baseList.get(index).merge(overrideEntry)
          baseList.set(index, mergedEntry)
        case None =>
          // Entry does not exist in base list, add it
          baseList.add(overrideEntry)
      }
    }
  }

  /**
   * Merge another TuningConfigsProvider instance into this one.
   * The other instance's configs will override this instance's configs for matching keys.
   * @param other The other TuningConfigsProvider instance to merge
   */
  def merge(other: TuningConfigsProvider): Unit = {
    mergeInto(this.default, other.default)
    mergeInto(this.qualification, other.qualification)
    mergeInto(this.profiling, other.profiling)
  }

  private lazy val tuningConfigsMap: Map[String, TuningConfigEntry] = {
    // Select the appropriate tool list based on the autoTunerHelper
    val toolList = autoTunerHelper match {
      case Some(QualificationAutoTunerHelper) => qualification
      case Some(ProfilingAutoTunerHelper) => profiling
      case _ => throw new IllegalArgumentException(s"Invalid Tool Helper type: $autoTunerHelper")
    }
    // Merge the default list with the selected tool list and convert to a map
    mergeInto(default, toolList)
    default.asScala.map(e => e.name -> e).toMap
  }

  /**
   * Get a config entry by name.
   * @param key The config name (e.g., "HEAP_PER_CORE_MB")
   * @return The config entry
   */
  def getEntry(key: String): TuningConfigEntry = {
    tuningConfigsMap.get(key) match {
      case Some(entry) => entry
      case None => throw new IllegalArgumentException(s"Config '$key' not found")
    }
  }

  /**
   * Check if a config exists.
   * @param key The config name
   * @return True if the config exists, false otherwise
   */
  def hasConfig(key: String): Boolean = {
    tuningConfigsMap.contains(key)
  }

  /**
   * Get all available config names.
   * @return Set of all config names
   */
  def getConfigNames: Set[String] = {
    tuningConfigsMap.keySet
  }

  override def validate(): Unit = {
    default.asScala.foreach(_.validate())
    qualification.asScala.foreach(_.validate())
    profiling.asScala.foreach(_.validate())
  }
}

object TuningConfigsProvider {
  val DEFAULT_CONFIGS_FILE = "bootstrap/tuningConfigs.yaml"
}
