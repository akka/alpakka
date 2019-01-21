/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.orientdb

import com.orientechnologies.orient.core.db.OPartitionedDatabasePool

final class OrientDBSourceSettings private (
    val oDatabasePool: com.orientechnologies.orient.core.db.OPartitionedDatabasePool,
    val maxPartitionSize: Int,
    val maxPoolSize: Int,
    val skip: Int,
    val limit: Int
) {

  def withOrientDBCredentials(
      value: com.orientechnologies.orient.core.db.OPartitionedDatabasePool
  ): OrientDBSourceSettings =
    copy(oDatabasePool = value)
  def withMaxPartitionSize(value: Int): OrientDBSourceSettings = copy(maxPartitionSize = value)
  def withMaxPoolSize(value: Int): OrientDBSourceSettings = copy(maxPoolSize = value)
  def withSkip(value: Int): OrientDBSourceSettings = copy(skip = value)
  def withLimit(value: Int): OrientDBSourceSettings = copy(limit = value)

  private def copy(
      oDatabasePool: com.orientechnologies.orient.core.db.OPartitionedDatabasePool = oDatabasePool,
      maxPartitionSize: Int = maxPartitionSize,
      maxPoolSize: Int = maxPoolSize,
      skip: Int = skip,
      limit: Int = limit
  ): OrientDBSourceSettings = new OrientDBSourceSettings(
    oDatabasePool = oDatabasePool,
    maxPartitionSize = maxPartitionSize,
    maxPoolSize = maxPoolSize,
    skip = skip,
    limit = limit
  )

  override def toString =
    "OrientDBSourceSettings(" +
    s"oDatabasePool=$oDatabasePool," +
    s"maxPartitionSize=$maxPartitionSize," +
    s"maxPoolSize=$maxPoolSize," +
    s"skip=$skip," +
    s"limit=$limit" +
    ")"
}

object OrientDBSourceSettings {

  /** Scala API */
  def apply(oDatabasePool: OPartitionedDatabasePool): OrientDBSourceSettings = new OrientDBSourceSettings(
    oDatabasePool,
    Runtime.getRuntime.availableProcessors(),
    maxPoolSize = -1,
    skip = 0,
    limit = 10
  )

  /** Java API */
  def create(oDatabasePool: OPartitionedDatabasePool): OrientDBSourceSettings = apply(oDatabasePool)
}
