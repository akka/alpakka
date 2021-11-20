/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.cassandra

import akka.ConfigurationException
import akka.actor.{ActorSystem, ClassicActorSystemProvider}
import akka.discovery.Discovery
import akka.util.JavaDurationConverters._
import com.datastax.oss.driver.api.core.CqlSession
import com.typesafe.config.{Config, ConfigFactory}

import scala.collection.immutable
import scala.compat.java8.FutureConverters._
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, Future}

/**
 * [[https://doc.akka.io/docs/akka/current/discovery/index.html Akka Discovery]]
 * is enabled by setting the `service-discovery.name` in the given `CassandraSession` config.
 *
 * Akka Discovery overwrites the basic.contact-points` from the configuration with addresses
 * provided by the configured Akka Discovery mechanism.
 *
 * Example using config-based Akka Discovery:
 * {{{
 * akka {
 *   discovery.method = config
 * }
 * akka.discovery.config.services = {
 *   cassandra-service = {
 *     endpoints = [
 *       {
 *         host = "127.0.0.1"
 *         port = 9042
 *       },
 *       {
 *         host = "127.0.0.2"
 *         port = 9042
 *       }
 *     ]
 *   }
 * }
 * alpakka.cassandra {
 *   service-discovery.name ="cassandra-service"
 * }
 * }}}
 *
 * Look up this `CassandraSession` with
 * {{{
 * CassandraSessionRegistry
 *   .get(system)
 *   .sessionFor(CassandraSessionSettings.create())
 * }}}
 */
private[cassandra] object AkkaDiscoverySessionProvider {

  def connect(system: ActorSystem, config: Config)(implicit ec: ExecutionContext): Future[CqlSession] = {
    readNodes(config)(system, ec).flatMap { contactPoints =>
      val driverConfigWithContactPoints = ConfigFactory.parseString(s"""
        basic.contact-points = [${contactPoints.mkString("\"", "\", \"", "\"")}]
        """).withFallback(CqlSessionProvider.driverConfig(system, config))
      val driverConfigLoader = DriverConfigLoaderFromConfig.fromConfig(driverConfigWithContactPoints)
      CqlSession.builder().withConfigLoader(driverConfigLoader).buildAsync().toScala
    }
  }

  def connect(system: ClassicActorSystemProvider, config: Config)(implicit ec: ExecutionContext): Future[CqlSession] =
    connect(system.classicSystem, config)

  /**
   * Expect a `service` section in Config and use Akka Discovery to read the addresses for `name` within `lookup-timeout`.
   */
  private def readNodes(config: Config)(implicit system: ActorSystem,
                                        ec: ExecutionContext): Future[immutable.Seq[String]] = {
    val serviceConfig = config.getConfig("service-discovery")
    val serviceName = serviceConfig.getString("name")
    val lookupTimeout = serviceConfig.getDuration("lookup-timeout").asScala
    readNodes(serviceName, lookupTimeout)
  }

  /**
   * Use Akka Discovery to read the addresses for `serviceName` within `lookupTimeout`.
   */
  private def readNodes(
      serviceName: String,
      lookupTimeout: FiniteDuration
  )(implicit system: ActorSystem, ec: ExecutionContext): Future[immutable.Seq[String]] = {
    Discovery(system).discovery.lookup(serviceName, lookupTimeout).map { resolved =>
      resolved.addresses.map { target =>
        target.host + ":" + target.port.getOrElse {
          throw new ConfigurationException(
            s"Akka Discovery for Cassandra service [$serviceName] must provide a port for [${target.host}]"
          )
        }
      }
    }
  }

}
