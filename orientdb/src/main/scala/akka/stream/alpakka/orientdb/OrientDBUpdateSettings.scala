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
    val maxPoolSize: Int
) {

  def withOrientDBCredentials(
      value: com.orientechnologies.orient.core.db.OPartitionedDatabasePool
  ): OrientDBUpdateSettings = copy(oDatabasePool = value)
  def withMaxPartitionSize(value: Int): OrientDBUpdateSettings = copy(maxPartitionSize = value)
  def withMaxPoolSize(value: Int): OrientDBUpdateSettings = copy(maxPoolSize = value)

  private def copy(
      oDatabasePool: com.orientechnologies.orient.core.db.OPartitionedDatabasePool = oDatabasePool,
      maxPartitionSize: Int = maxPartitionSize,
      maxPoolSize: Int = maxPoolSize
  ): OrientDBUpdateSettings = new OrientDBUpdateSettings(
    oDatabasePool = oDatabasePool,
    maxPartitionSize = maxPartitionSize,
    maxPoolSize = maxPoolSize
  )

  override def toString =
    "OrientDBUpdateSettings(" +
    s"oDatabasePool=$oDatabasePool," +
    s"maxPartitionSize=$maxPartitionSize," +
    s"maxPoolSize=$maxPoolSize" +
    ")"
}

object OrientDBUpdateSettings {

  /** Scala API */
  def apply(oDatabasePool: OPartitionedDatabasePool): OrientDBUpdateSettings =
    new OrientDBUpdateSettings(
      oDatabasePool: OPartitionedDatabasePool,
      maxPartitionSize = Runtime.getRuntime.availableProcessors(),
      maxPoolSize = -1
    )

  /** Java API */
  def create(oDatabasePool: OPartitionedDatabasePool): OrientDBUpdateSettings = apply(oDatabasePool)
}
