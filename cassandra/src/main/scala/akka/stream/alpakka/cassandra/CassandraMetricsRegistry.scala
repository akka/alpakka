/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.cassandra

import akka.actor.{ClassicActorSystemProvider, ExtendedActorSystem, Extension, ExtensionId, ExtensionIdProvider}
import akka.annotation.InternalApi
import com.codahale.metrics.MetricRegistry

import scala.jdk.CollectionConverters._

/**
 * Retrieves Cassandra metrics registry for an actor system
 */
class CassandraMetricsRegistry extends Extension {
  private val metricRegistry = new MetricRegistry()

  def getRegistry: MetricRegistry = metricRegistry

  /**
   * INTERNAL API
   */
  @InternalApi private[akka] def addMetrics(category: String, registry: MetricRegistry): Unit =
    metricRegistry.register(category, registry)

  /**
   * INTERNAL API
   */
  @InternalApi private[akka] def removeMetrics(category: String): Unit =
    metricRegistry.getNames.iterator.asScala.foreach { name =>
      if (name.startsWith(category))
        metricRegistry.remove(name)
    }
}

object CassandraMetricsRegistry extends ExtensionId[CassandraMetricsRegistry] with ExtensionIdProvider {
  override def lookup = CassandraMetricsRegistry
  override def createExtension(system: ExtendedActorSystem) =
    new CassandraMetricsRegistry

  /**
   * Java API.
   * Get the CassandraMetricsRegistry extension with the classic actors API.
   */
  override def get(system: akka.actor.ActorSystem): CassandraMetricsRegistry = super.apply(system)

  /**
   * Java API.
   * Get the CassandraMetricsRegistry extension with the classic actors API.
   */
  override def get(system: ClassicActorSystemProvider): CassandraMetricsRegistry = super.apply(system)
}
