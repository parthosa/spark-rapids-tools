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

package org.apache.spark.sql.rapids.tool.util

import java.io.Serializable
import java.nio.ByteBuffer

import scala.collection.mutable

import org.apache.commons.lang3.SerializationUtils
import org.rocksdb.{Options, RocksDB, RocksDBException}

object RocksDBClient {
  RocksDB.loadLibrary()
}

class RocksDBClient[V <: Serializable](basePath: String)
  extends mutable.HashMap[Long, V] {
  private val threadId = Thread.currentThread().getId
  private val dbPath = s"${basePath}_thread_$threadId"
  private val options = new Options().setCreateIfMissing(true)
  private val db = RocksDB.open(options, dbPath)

  override def get(key: Long): Option[V] = {
    try {
      val keyBytes = serializeLong(key)
      val valueBytes = db.get(keyBytes)
      if (valueBytes != null) Some(SerializationUtils.deserialize[V](valueBytes))
      else None
    } catch {
      case e: RocksDBException =>
        println(s"Failed to get data: ${e.getMessage}")
        None
    }
  }

  override def +=(kv: (Long, V)): this.type = {
    val (key, value) = kv
    try {
      val keyBytes = serializeLong(key)
      val valueBytes = SerializationUtils.serialize(value)
      db.put(keyBytes, valueBytes)
    } catch {
      case e: RocksDBException =>
        println(s"Failed to put data: ${e.getMessage}")
    }
    this
  }

  override def -=(key: Long): this.type = {
    try {
      val keyBytes = serializeLong(key)
      db.delete(keyBytes)
    } catch {
      case e: RocksDBException =>
        println(s"Failed to delete data: ${e.getMessage}")
    }
    this
  }

  private def serializeLong(value: Long): Array[Byte] = {
    ByteBuffer.allocate(8).putLong(value).array()
  }

  def close(): Unit = {
    db.close()
    options.close()
  }
}
