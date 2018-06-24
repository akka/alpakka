/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.postgresqlcdc.scaladsl

import scala.concurrent.duration._

sealed trait Plugin

object Plugins {

  case object TestDecoding extends Plugin

  case object Wal2Json extends Plugin

}

/** Settings for PostgreSQL CDC
 *
 * @param connectionString PostgreSQL JDBC connection string
 * @param slotName         Name of the "logical decoding" slot
 * @param maxItems         Specifies how many rows are fetched in one batch
 * @param duration         Duration between polls
 */
final case class PostgreSQLInstance(connectionString: String,
                                    slotName: String,
                                    plugin: Plugin,
                                    maxItems: Int = 128,
                                    duration: FiniteDuration = 2000.milliseconds)
