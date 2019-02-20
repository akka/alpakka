/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.orientdb

import com.orientechnologies.orient.core.db.OPartitionedDatabasePool

final class OrientDbWriteSettings private (
    val oDatabasePool: com.orientechnologies.orient.core.db.OPartitionedDatabasePool
) {

  def withOrientDBCredentials(
      value: com.orientechnologies.orient.core.db.OPartitionedDatabasePool
  ): OrientDbWriteSettings = copy(oDatabasePool = value)

  private def copy(
      oDatabasePool: com.orientechnologies.orient.core.db.OPartitionedDatabasePool = oDatabasePool
  ): OrientDbWriteSettings = new OrientDbWriteSettings(
    oDatabasePool = oDatabasePool
  )

  override def toString =
    "OrientDBUpdateSettings(" +
    s"oDatabasePool=$oDatabasePool" +
    ")"
}

object OrientDbWriteSettings {

  /** Scala API */
  def apply(oDatabasePool: OPartitionedDatabasePool): OrientDbWriteSettings =
    new OrientDbWriteSettings(
      oDatabasePool: OPartitionedDatabasePool
    )

  /** Java API */
  def create(oDatabasePool: OPartitionedDatabasePool): OrientDbWriteSettings = apply(oDatabasePool)
}
