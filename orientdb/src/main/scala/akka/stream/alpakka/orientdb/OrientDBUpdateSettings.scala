/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.orientdb

import com.orientechnologies.orient.core.db.OPartitionedDatabasePool

import scala.concurrent.duration._

//#sink-settings
final case class OrientDBUpdateSettings(oDatabasePool: OPartitionedDatabasePool,
                                        maxPartitionSize: Int = Runtime.getRuntime.availableProcessors(),
                                        maxPoolSize: Int = -1,
                                        maxRetry: Int = 1,
                                        retryInterval: FiniteDuration = 5000 millis,
                                        bufferSize: Int = 10)
//#sink-settings
{

  def withMaxPartitionSize(maxPartitionSize: Int): OrientDBUpdateSettings =
    copy(maxPartitionSize = maxPartitionSize)

  def withMaxPoolSize(maxPoolSize: Int): OrientDBUpdateSettings =
    copy(maxPoolSize = maxPoolSize)

  def withRetries(maxRetry: Int, retryInterval: Long, timeUnit: TimeUnit): OrientDBUpdateSettings =
    copy(maxRetry = maxRetry, retryInterval = Duration(retryInterval, timeUnit))

  def withBufferSize(bufferSize: Int): OrientDBUpdateSettings =
    copy(bufferSize = bufferSize)

  def withOrientDBCredentials(oDatabasePool: OPartitionedDatabasePool): OrientDBUpdateSettings =
    copy(oDatabasePool = oDatabasePool)
}

object OrientDBUpdateSettings {

  def create(oDatabasePool: OPartitionedDatabasePool) =
    OrientDBUpdateSettings(oDatabasePool)
}
