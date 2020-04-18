/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.debezium

import java.time.Duration
import java.util
import java.util.Properties
import java.util.concurrent.TimeUnit

import io.debezium.engine.spi.OffsetCommitPolicy
import org.apache.kafka.connect.connector.Connector
import org.apache.kafka.connect.storage.{Converter, OffsetBackingStore}

import scala.concurrent.duration.FiniteDuration
import scala.collection.JavaConverters._

final class DebeziumSettings private (
    val name: String,
    val connectorClass: Class[_ <: Connector],
    val offsetStorage: Class[_ <: OffsetBackingStore],
    val offsetStorageFileName: String,
    val offsetCommitPolicy: Class[_ <: OffsetCommitPolicy],
    val offsetFlushInterval: FiniteDuration,
    val offsetFlushTimeout: FiniteDuration,
    val internalKeyConverter: Class[_ <: Converter],
    val internalValueConverter: Class[_ <: Converter],
    val customSettings: Map[String, Any],
    val maxInMemoryElements: Int,
    val acknowledgmentTimeout: FiniteDuration
) {

  def copy(
      name: String = name,
      connectorClass: Class[_ <: Connector] = connectorClass,
      offsetStorage: Class[_ <: OffsetBackingStore] = offsetStorage,
      offsetStorageFileName: String = offsetStorageFileName,
      offsetCommitPolicy: Class[_ <: OffsetCommitPolicy] = offsetCommitPolicy,
      offsetFlushInterval: FiniteDuration = offsetFlushInterval,
      offsetFlushTimeout: FiniteDuration = offsetFlushTimeout,
      internalKeyConverter: Class[_ <: Converter] = internalKeyConverter,
      internalValueConverter: Class[_ <: Converter] = internalValueConverter,
      customSettings: Map[String, Any] = customSettings,
      maxInMemoryElements: Int = maxInMemoryElements,
      acknowledgmentTimeout: FiniteDuration = acknowledgmentTimeout
  ): DebeziumSettings =
    new DebeziumSettings(name,
                         connectorClass,
                         offsetStorage,
                         offsetStorageFileName,
                         offsetCommitPolicy,
                         offsetFlushInterval,
                         offsetFlushTimeout,
                         internalKeyConverter,
                         internalValueConverter,
                         customSettings,
                         maxInMemoryElements,
                         acknowledgmentTimeout)

  def properties: Properties = {
    val properties = new Properties()

    properties.put("name", name)
    properties.put("connector.class", connectorClass.getName)
    properties.put("offset.storage", offsetStorage)
    properties.put("offset.storage.file.filename", offsetStorageFileName)
    properties.put("offset.commit.policy", offsetCommitPolicy.getName)
    properties.put("offset.flush.interval.ms", s"${offsetFlushInterval.toMillis}")
    properties.put("offset.flush.timeout.ms", s"${offsetFlushTimeout.toMillis}")
    properties.put("internal.key.converter", internalKeyConverter.getName)
    properties.put("internal.value.converter", internalValueConverter.getName)

    val custom: util.Map[Object, Object] = customSettings.asJava.asInstanceOf[util.Map[Object, Object]]
    properties.putAll(custom)

    properties
  }
}

object DebeziumSettings {
  import scala.collection.JavaConverters._

  def apply(
      name: String,
      connectorClass: Class[_ <: Connector],
      offsetStorage: Class[_ <: OffsetBackingStore],
      offsetStorageFileName: String,
      offsetCommitPolicy: Class[_ <: OffsetCommitPolicy],
      offsetFlushInterval: FiniteDuration,
      offsetFlushTimeout: FiniteDuration,
      internalKeyConverter: Class[_ <: Converter],
      internalValueConverter: Class[_ <: Converter],
      customSettings: Map[String, Any],
      maxInMemoryElements: Int,
      acknowledgmentTimeout: FiniteDuration
  ): DebeziumSettings =
    new DebeziumSettings(name,
                         connectorClass,
                         offsetStorage,
                         offsetStorageFileName,
                         offsetCommitPolicy,
                         offsetFlushInterval,
                         offsetFlushTimeout,
                         internalKeyConverter,
                         internalValueConverter,
                         customSettings,
                         maxInMemoryElements,
                         acknowledgmentTimeout)

  def create(name: String,
             connectorClass: Class[_ <: Connector],
             offsetStorage: Class[_ <: OffsetBackingStore],
             offsetStorageFileName: String,
             offsetCommitPolicy: Class[_ <: OffsetCommitPolicy],
             offsetFlushInterval: Duration,
             offsetFlushTimeout: Duration,
             internalKeyConverter: Class[_ <: Converter],
             internalValueConverter: Class[_ <: Converter],
             customSettings: util.HashMap[String, Object],
             maxInMemoryElements: Int,
             acknowledgmentTimeout: Duration): DebeziumSettings = {

    def asScala(d: Duration) = FiniteDuration(d.toMillis, TimeUnit.MILLISECONDS)

    new DebeziumSettings(name,
                         connectorClass,
                         offsetStorage,
                         offsetStorageFileName,
                         offsetCommitPolicy,
                         asScala(offsetFlushInterval),
                         asScala(offsetFlushTimeout),
                         internalKeyConverter,
                         internalValueConverter,
                         customSettings.asScala.toMap,
                         maxInMemoryElements,
                         asScala(acknowledgmentTimeout))
  }
}
