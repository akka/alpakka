/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.postgresqlcdc.scaladsl

import scala.concurrent.duration._

sealed trait Mode

object Mode {

  case object Get extends Mode

  case class Peek(fromLsn: Long) extends Mode

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
                                    mode: Mode = Mode.Get,
                                    maxItems: Int = 128,
                                    duration: FiniteDuration = 2000.milliseconds)
