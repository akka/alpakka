/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.cassandra

import akka.actor.{ActorSystem, ExtendedActorSystem}
import com.datastax.oss.driver.api.core.CqlSession
import com.typesafe.config.{Config, ConfigFactory}

import scala.collection.immutable
import scala.compat.java8.FutureConverters._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Failure

/**
 * The implementation of the `SessionProvider` is used for creating the
 * Cassandra Session. By default the [[DefaultSessionProvider]] is building
 * the Cluster from configuration properties but it is possible to
 * replace the implementation of the SessionProvider to reuse another
 * session or override the Cluster builder with other settings.
 *
 * The implementation is defined in configuration `session-provider` property.
 * It may optionally have a constructor with an ActorSystem and Config parameter.
 * The config parameter is the config section of the plugin.
 */
trait CqlSessionProvider {
  def connect()(implicit ec: ExecutionContext): Future[CqlSession]
}

/**
 * Builds a `CqlSession` from the given `config` via [[DriverConfigLoaderFromConfig]].
 *
 * The configuration for the driver is typically the `datastax-java-driver` section of the ActorSystem's
 * configuration, but it's possible to use other configuration. The configuration path of the
 * driver's configuration can be defined with `datastax-java-driver-config` property in the
 * given `config`.
 */
class DefaultSessionProvider(system: ActorSystem, config: Config) extends CqlSessionProvider {

  /**
   * Check if Akka Discovery service lookup should be used. It is part of this class so it
   * doesn't trigger the [[AkkaDiscoverySessionProvider]] class to be loaded.
   */
  private def useAkkaDiscovery(config: Config): Boolean = config.getString("service-discovery.name").nonEmpty

  override def connect()(implicit ec: ExecutionContext): Future[CqlSession] = {
    if (useAkkaDiscovery(config)) {
      AkkaDiscoverySessionProvider.connect(system, config)
    } else {
      val driverConfig = CqlSessionProvider.driverConfig(system, config)
      val driverConfigLoader = DriverConfigLoaderFromConfig.fromConfig(driverConfig)
      CqlSession.builder().withConfigLoader(driverConfigLoader).buildAsync().toScala
    }
  }
}

object CqlSessionProvider {

  /**
   * Create a `SessionProvider` from configuration.
   * The `session-provider` config property defines the fully qualified
   * class name of the SessionProvider implementation class. It may optionally
   * have a constructor with an `ActorSystem` and `Config` parameter.
   */
  def apply(system: ExtendedActorSystem, config: Config): CqlSessionProvider = {
    val className = config.getString("session-provider")
    val dynamicAccess = system.asInstanceOf[ExtendedActorSystem].dynamicAccess
    val clazz = dynamicAccess.getClassFor[CqlSessionProvider](className).get
    def instantiate(args: immutable.Seq[(Class[_], AnyRef)]) =
      dynamicAccess.createInstanceFor[CqlSessionProvider](clazz, args)

    val params = List((classOf[ActorSystem], system), (classOf[Config], config))
    instantiate(params)
      .recoverWith {
        case x: NoSuchMethodException => instantiate(params.take(1))
      }
      .recoverWith { case x: NoSuchMethodException => instantiate(Nil) }
      .recoverWith {
        case ex: Exception =>
          Failure(
            new IllegalArgumentException(
              s"Unable to create SessionProvider instance for class [$className], " +
              "tried constructor with ActorSystem, Config, and only ActorSystem, and no parameters",
              ex
            )
          )
      }
      .get
  }

  /**
   * The `Config` for the `datastax-java-driver`. The configuration path of the
   * driver's configuration can be defined with `datastax-java-driver-config` property in the
   * given `config`. `datastax-java-driver` configuration section is also used as fallback.
   */
  def driverConfig(system: ActorSystem, config: Config): Config = {
    val driverConfigPath = config.getString("datastax-java-driver-config")
    system.settings.config.getConfig(driverConfigPath).withFallback {
      if (driverConfigPath == "datastax-java-driver") ConfigFactory.empty()
      else system.settings.config.getConfig("datastax-java-driver")
    }
  }
}
