/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.cassandra

import java.util.concurrent.CompletableFuture
import java.util.concurrent.CompletionStage

import com.datastax.oss.driver.api.core.config.DriverConfig
import com.datastax.oss.driver.api.core.config.DriverConfigLoader
import com.datastax.oss.driver.api.core.context.DriverContext
import com.datastax.oss.driver.internal.core.config.typesafe.TypesafeDriverConfig
import com.typesafe.config.Config

object DriverConfigLoaderFromConfig {
  def fromConfig(config: Config): DriverConfigLoader =
    new DriverConfigLoaderFromConfig(config)
}

/**
 * `DriverConfigLoader` that reads the settings of the Cassandra driver from a
 * given `Config`. The `DefaultDriverConfigLoader` loads `application.conf` or file,
 * which is not necessarily the same as the ActorSystem's configuration.
 *
 * The [[DefaultSessionProvider]] is using this when building `CqlSession`.
 *
 * Intended to be used with `CqlSession.builder().withConfigLoader` when implementing
 * a custom [[CqlSessionProvider]].
 */
class DriverConfigLoaderFromConfig(config: Config) extends DriverConfigLoader {

  private val driverConfig: DriverConfig = new TypesafeDriverConfig(config)

  override def getInitialConfig: DriverConfig = {
    driverConfig
  }

  override def onDriverInit(context: DriverContext): Unit = ()

  override def reload(): CompletionStage[java.lang.Boolean] =
    CompletableFuture.completedFuture(false)

  override def supportsReloading(): Boolean = false

  override def close(): Unit = ()
}
