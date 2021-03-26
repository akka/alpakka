/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.bigquery

import akka.actor.ClassicActorSystemProvider
import akka.annotation.InternalApi
import akka.util.JavaDurationConverters._
import com.typesafe.config.Config

import java.time
import scala.concurrent.duration._

object BigQuerySettings {
  val ConfigPath = "alpakka.google.bigquery"

  /**
   * Reads from the given config.
   */
  def apply(c: Config): BigQuerySettings =
    BigQuerySettings(c.getDuration("load-job-per-table-quota").asScala)

  /**
   * Java API: Reads from the given config.
   */
  def create(c: Config) = apply(c)

  /**
   * Scala API: Creates [[BigQuerySettings]] from the [[com.typesafe.config.Config Config]] attached to an actor system.
   */
  def apply()(implicit system: ClassicActorSystemProvider, dummy: DummyImplicit): BigQuerySettings = apply(system)

  /**
   * Scala API: Creates [[BigQuerySettings]] from the [[com.typesafe.config.Config Config]] attached to an [[akka.actor.ActorSystem]].
   */
  def apply(system: ClassicActorSystemProvider): BigQuerySettings = BigQueryExt(system.classicSystem).settings

  implicit def settings(implicit system: ClassicActorSystemProvider): BigQuerySettings = apply(system)

  /**
   * Java API: Creates [[BigQuerySettings]] from the [[com.typesafe.config.Config Config]] attached to an actor system.
   */
  def create(system: ClassicActorSystemProvider): BigQuerySettings = apply(system)

  /**
   * Java API
   */
  def create(loadJobPerTableQuota: time.Duration) = BigQuerySettings(loadJobPerTableQuota.asScala)

}

final case class BigQuerySettings @InternalApi private (loadJobPerTableQuota: FiniteDuration) {
  def getLoadJobPerTableQuota = loadJobPerTableQuota.asJava
  def withLoadJobPerTableQuota(loadJobPerTableQuota: FiniteDuration) =
    copy(loadJobPerTableQuota = loadJobPerTableQuota)
  def withLoadJobPerTableQuota(loadJobPerTableQuota: time.Duration) =
    copy(loadJobPerTableQuota = loadJobPerTableQuota.asScala)
}
