/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.postgresqlcdc.scaladsl

// rename Java imports if the name clashes with the Scala name
import java.time.{Duration => JavaDuration}

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
final class PostgreSQLInstance private (val connectionString: String = null,
                                        val slotName: String = null,
                                        val mode: Mode = Mode.Get,
                                        val maxItems: Int = 128,
                                        val duration: FiniteDuration = 2000.milliseconds) {

  def withConnectionString(connectionString: String): PostgreSQLInstance =
    copy(connectionString = connectionString)

  def withSlotName(slotName: String): PostgreSQLInstance =
    copy(slotName = slotName)

  def withMode(mode: Mode): PostgreSQLInstance =
    copy(mode = mode)

  def withMaxItems(maxItems: Int): PostgreSQLInstance =
    copy(maxItems = maxItems)

  def withDuration(duration: FiniteDuration): PostgreSQLInstance =
    copy(duration = duration)

  /**
   * Java API
   */
  def withPollInterval(duration: JavaDuration): PostgreSQLInstance = {
    import scala.compat.java8.DurationConverters._
    copy(duration = duration.toScala)
  }

  private def copy[M, N](connectionString: String = connectionString,
                         slotName: String = slotName,
                         mode: Mode = mode,
                         maxItems: Int = maxItems,
                         duration: FiniteDuration = duration): PostgreSQLInstance =
    new PostgreSQLInstance(connectionString, slotName, mode, maxItems, duration)

  override def toString: String =
    s"""
      |PostgreSQLInstance(
      | connectionString = $connectionString
      | slotName = $slotName
      | mode = $Mode
      | maxItems = $maxItems
      | duration = $duration
      |)""".stripMargin

}

object PostgreSQLInstance {

  /**
   * Factory method for Scala.
   */
  def apply(): PostgreSQLInstance = new PostgreSQLInstance()

  /**
   * Java API
   *
   * Factory method for Java.
   */
  def create(): PostgreSQLInstance = PostgreSQLInstance()
}
