/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.orientdb

import com.orientechnologies.orient.core.db.OPartitionedDatabasePool

//#source-settings
final case class OrientDBSourceSettings(oDatabasePool: OPartitionedDatabasePool,
                                        maxPartitionSize: Int = Runtime.getRuntime.availableProcessors(),
                                        maxPoolSize: Int = -1,
                                        skip: Int = 0,
                                        limit: Int = 10)
//#source-settings
{

  def withMaxPartitionSize(maxPartitionSize: Int): OrientDBSourceSettings =
    copy(maxPartitionSize = maxPartitionSize)

  def withMaxPoolSize(maxPoolSize: Int): OrientDBSourceSettings =
    copy(maxPoolSize = maxPoolSize)

  def withSkipAndLimit(skip: Int, limit: Int): OrientDBSourceSettings =
    copy(skip = skip, limit = limit)

  def withOrientDBCredentials(oDatabasePool: OPartitionedDatabasePool): OrientDBSourceSettings =
    copy(oDatabasePool = oDatabasePool)
}

object OrientDBSourceSettings {

  def create(oDatabasePool: OPartitionedDatabasePool) =
    OrientDBSourceSettings(oDatabasePool)
}
