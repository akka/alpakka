/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.cassandra

import java.util.concurrent.CompletionStage

import akka.Done
import com.datastax.oss.driver.api.core.CqlSession
import scala.compat.java8.FunctionConverters._
import scala.compat.java8.FutureConverters._

import scala.concurrent.Future

class CassandraSessionSettings private (val configPath: String,
                                        _metricsCategory: Option[String] = None,
                                        val init: Option[CqlSession => Future[Done]] = None
) {

  def metricsCategory: String = _metricsCategory.getOrElse(configPath)

  def withMetricCategory(value: String): CassandraSessionSettings =
    copy(metricsCategory = Option(value))

  /**
   * Java API:
   *
   * The `init` function will be performed once when the session is created, i.e.
   * if `CassandraSessionRegistry.sessionFor` is called from multiple places with different `init` it will
   * only execute the first.
   */
  def withInit(value: java.util.function.Function[CqlSession, CompletionStage[Done]]): CassandraSessionSettings =
    copy(init = Some(value.asScala.andThen(_.toScala)))

  /**
   * The `init` function will be performed once when the session is created, i.e.
   * if `CassandraSessionRegistry.sessionFor` is called from multiple places with different `init` it will
   * only execute the first.
   */
  def withInit(value: CqlSession => Future[Done]): CassandraSessionSettings = copy(init = Some(value))

  private def copy(configPath: String = configPath,
                   metricsCategory: Option[String] = _metricsCategory,
                   init: Option[CqlSession => Future[Done]] = init
  ) =
    new CassandraSessionSettings(configPath, metricsCategory, init)

  override def toString: String =
    "CassandraSessionSettings(" +
    s"configPath=$configPath," +
    s"metricsCategory=$metricsCategory," +
    s"init=$init)"
}

object CassandraSessionSettings {

  val ConfigPath = "alpakka.cassandra"

  def apply(): CassandraSessionSettings = apply(ConfigPath)

  def apply(configPath: String, init: CqlSession => Future[Done]): CassandraSessionSettings =
    new CassandraSessionSettings(configPath, init = Some(init))

  def apply(configPath: String): CassandraSessionSettings = new CassandraSessionSettings(configPath)

  /** Java API */
  def create(): CassandraSessionSettings = apply(ConfigPath)

  /** Java API */
  def create(configPath: String): CassandraSessionSettings = apply(configPath)
}
