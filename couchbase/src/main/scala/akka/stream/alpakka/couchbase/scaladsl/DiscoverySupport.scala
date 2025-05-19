/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.couchbase.scaladsl

import java.util.concurrent.CompletionStage

import akka.actor.{ActorSystem, ClassicActorSystemProvider}
import akka.annotation.InternalApi
import akka.discovery.Discovery
import akka.stream.alpakka.couchbase.CouchbaseSessionSettings
import com.typesafe.config.Config

import scala.collection.immutable
import scala.jdk.DurationConverters._
import scala.jdk.FunctionConverters._
import scala.jdk.FutureConverters._
import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration

/**
 * Utility to delegate Couchbase node address lookup to [[https://doc.akka.io/libraries/akka-core/current/discovery/index.html Akka Discovery]].
 */
sealed class DiscoverySupport private {

  /**
   * Use Akka Discovery to read the addresses for `serviceName` within `lookupTimeout`.
   */
  private def readNodes(
      serviceName: String,
      lookupTimeout: FiniteDuration
  )(implicit system: ClassicActorSystemProvider): Future[immutable.Seq[String]] = {
    implicit val ec = system.classicSystem.dispatcher
    val discovery = Discovery(system).discovery
    discovery.lookup(serviceName, lookupTimeout).map { resolved =>
      resolved.addresses.map(_.host)
    }
  }

  /**
   * Expect a `service` section in Config and use Akka Discovery to read the addresses for `name` within `lookup-timeout`.
   */
  private def readNodes(config: Config)(implicit system: ClassicActorSystemProvider): Future[immutable.Seq[String]] =
    if (config.hasPath("service")) {
      val serviceName = config.getString("service.name")
      val lookupTimeout = config.getDuration("service.lookup-timeout").toScala
      readNodes(serviceName, lookupTimeout)
    } else throw new IllegalArgumentException(s"config $config does not contain `service` section")

  /**
   * Expects a `service` section in the given Config and reads the given service name's address
   * to be used as Couchbase `nodes`.
   */
  def nodes(
      config: Config
  )(implicit system: ClassicActorSystemProvider): CouchbaseSessionSettings => Future[CouchbaseSessionSettings] = {
    implicit val ec = system.classicSystem.dispatcher
    settings =>
      readNodes(config)
        .map { nodes =>
          settings.withNodes(nodes)
        }
  }

  private[couchbase] def nodes(config: Config,
                               system: ActorSystem): CouchbaseSessionSettings => Future[CouchbaseSessionSettings] =
    nodes(config)(system)

  /**
   * Internal API: Java wrapper.
   */
  @InternalApi
  private[couchbase] def getNodes(
      config: Config,
      system: ClassicActorSystemProvider
  ): java.util.function.Function[CouchbaseSessionSettings, CompletionStage[CouchbaseSessionSettings]] =
    nodes(config)(system).andThen(_.asJava).asJava

  /**
   * Expects a `service` section in `alpakka.couchbase.session` and reads the given service name's address
   * to be used as Couchbase `nodes`.
   */
  def nodes()(
      implicit system: ClassicActorSystemProvider
  ): CouchbaseSessionSettings => Future[CouchbaseSessionSettings] =
    nodes(system.classicSystem)

  /**
   * Expects a `service` section in `alpakka.couchbase.session` and reads the given service name's address
   * to be used as Couchbase `nodes`.
   */
  def nodes(system: ActorSystem): CouchbaseSessionSettings => Future[CouchbaseSessionSettings] =
    nodes(system.settings.config.getConfig(CouchbaseSessionSettings.configPath))(system)

}

/**
 * Utility to delegate Couchbase node address lookup to [[https://doc.akka.io/libraries/akka-core/current/discovery/index.html Akka Discovery]].
 */
object DiscoverySupport extends DiscoverySupport {

  /** Internal API */
  @InternalApi val INSTANCE: DiscoverySupport = this
}
