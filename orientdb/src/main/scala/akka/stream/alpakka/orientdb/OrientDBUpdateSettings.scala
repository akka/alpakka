/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.orientdb

import com.orientechnologies.orient.core.db.OPartitionedDatabasePool

import scala.concurrent.duration._
import akka.util.JavaDurationConverters._

final class OrientDBUpdateSettings private (
    val oDatabasePool: com.orientechnologies.orient.core.db.OPartitionedDatabasePool,
    val maxPartitionSize: Int,
    val maxPoolSize: Int,
    val maxRetry: Int,
    val retryInterval: scala.concurrent.duration.FiniteDuration,
    val bufferSize: Int
) {

  def withOrientDBCredentials(
      value: com.orientechnologies.orient.core.db.OPartitionedDatabasePool
  ): OrientDBUpdateSettings = copy(oDatabasePool = value)
  def withMaxPartitionSize(value: Int): OrientDBUpdateSettings = copy(maxPartitionSize = value)
  def withMaxPoolSize(value: Int): OrientDBUpdateSettings = copy(maxPoolSize = value)
  def withMaxRetries(value: Int): OrientDBUpdateSettings = copy(maxRetry = value)

  /** Scala API */
  def withRetryInterval(value: scala.concurrent.duration.FiniteDuration): OrientDBUpdateSettings =
    copy(retryInterval = value)

  /** Java API */
  def withRetryInterval(value: java.time.Duration): OrientDBUpdateSettings = copy(retryInterval = value.asScala)
  def withBufferSize(value: Int): OrientDBUpdateSettings = copy(bufferSize = value)

  private def copy(
      oDatabasePool: com.orientechnologies.orient.core.db.OPartitionedDatabasePool = oDatabasePool,
      maxPartitionSize: Int = maxPartitionSize,
      maxPoolSize: Int = maxPoolSize,
      maxRetry: Int = maxRetry,
      retryInterval: scala.concurrent.duration.FiniteDuration = retryInterval,
      bufferSize: Int = bufferSize
  ): OrientDBUpdateSettings = new OrientDBUpdateSettings(
    oDatabasePool = oDatabasePool,
    maxPartitionSize = maxPartitionSize,
    maxPoolSize = maxPoolSize,
    maxRetry = maxRetry,
    retryInterval = retryInterval,
    bufferSize = bufferSize
  )

  override def toString =
    "OrientDBUpdateSettings(" +
    s"oDatabasePool=$oDatabasePool," +
    s"maxPartitionSize=$maxPartitionSize," +
    s"maxPoolSize=$maxPoolSize," +
    s"maxRetry=$maxRetry," +
    s"retryInterval=${retryInterval.toCoarsest}," +
    s"bufferSize=$bufferSize" +
    ")"
}

object OrientDBUpdateSettings {

  /** Scala API */
  def apply(oDatabasePool: OPartitionedDatabasePool): OrientDBUpdateSettings =
    new OrientDBUpdateSettings(
      oDatabasePool: OPartitionedDatabasePool,
      maxPartitionSize = Runtime.getRuntime.availableProcessors(),
      maxPoolSize = -1,
      maxRetry = 1,
      retryInterval = 5.seconds,
      bufferSize = 10
    )

  /** Java API */
  def create(oDatabasePool: OPartitionedDatabasePool): OrientDBUpdateSettings = apply(oDatabasePool)
}
