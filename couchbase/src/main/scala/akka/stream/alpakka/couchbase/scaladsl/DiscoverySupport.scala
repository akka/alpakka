/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.couchbase.scaladsl

import java.util.concurrent.CompletionStage

import akka.actor.ActorSystem
import akka.annotation.InternalApi
import akka.discovery.Discovery
import akka.stream.alpakka.couchbase.CouchbaseSessionSettings
import akka.util.JavaDurationConverters._
import com.typesafe.config.Config

import scala.collection.immutable
import scala.compat.java8.FunctionConverters._
import scala.compat.java8.FutureConverters._
import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration

/**
 * Utility to delegate Couchbase node address lookup to [[https://doc.akka.io/docs/akka/current/discovery/index.html Akka Discovery]].
 */
sealed class DiscoverySupport private {

  /**
   * Use Akka Discovery to read the addresses for `serviceName` within `lookupTimeout`.
   */
  private def readNodes(
      serviceName: String,
      lookupTimeout: FiniteDuration
  )(implicit system: ActorSystem): Future[immutable.Seq[String]] = {
    import system.dispatcher
    val discovery = Discovery(system).discovery
    discovery.lookup(serviceName, lookupTimeout).map { resolved =>
      resolved.addresses.map(_.host)
    }
  }

  /**
   * Expect a `service` section in Config and use Akka Discovery to read the addresses for `name` within `lookup-timeout`.
   */
  private def readNodes(config: Config)(implicit system: ActorSystem): Future[immutable.Seq[String]] =
    if (config.hasPath("service")) {
      val serviceName = config.getString("service.name")
      val lookupTimeout = config.getDuration("service.lookup-timeout").asScala
      readNodes(serviceName, lookupTimeout)
    } else throw new IllegalArgumentException(s"config $config does not contain `service` section")

  /**
   * Expects a `service` section in the given Config and reads the given service name's address
   * to be used as Couchbase `nodes`.
   */
  def nodes(
      config: Config
  )(implicit system: ActorSystem): CouchbaseSessionSettings => Future[CouchbaseSessionSettings] = {
    import system.dispatcher
    settings =>
      readNodes(config)
        .map { nodes =>
          settings.withNodes(nodes)
        }
  }

  /**
   * Internal API: Java wrapper.
   */
  @InternalApi
  private[couchbase] def getNodes(
      config: Config,
      system: ActorSystem
  ): java.util.function.Function[CouchbaseSessionSettings, CompletionStage[CouchbaseSessionSettings]] =
    nodes(config)(system).andThen(_.toJava).asJava

  /**
   * Expects a `service` section in `alpakka.couchbase.session` and reads the given service name's address
   * to be used as Couchbase `nodes`.
   */
  def nodes()(implicit system: ActorSystem): CouchbaseSessionSettings => Future[CouchbaseSessionSettings] =
    nodes(system.settings.config.getConfig(CouchbaseSessionSettings.configPath))(system)

}

/**
 * Utility to delegate Couchbase node address lookup to [[https://doc.akka.io/docs/akka/current/discovery/index.html Akka Discovery]].
 */
object DiscoverySupport extends DiscoverySupport {
  /** Internal API */
  @InternalApi private[couchbase] val INSTANCE: DiscoverySupport = this
}
