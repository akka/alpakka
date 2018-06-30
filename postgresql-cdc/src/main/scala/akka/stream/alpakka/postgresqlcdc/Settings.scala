/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.postgresqlcdc

// rename Java imports if the name clashes with the Scala name
import java.time.{Duration => JavaDuration}
import java.util.{List => JavaList, Map => JavaMap}

import scala.collection.JavaConverters._
import scala.concurrent.duration._

sealed trait Mode

object Mode {

  case object Get extends Mode

  case class Peek(fromLsn: Long) extends Mode

}

/** Settings for PostgreSQL CDC
 *
 * @param connectionString  PostgreSQL JDBC connection string
 * @param slotName          Name of the "logical decoding" slot
 * @param mode              Choose between "at most once delivery" / "at least once"
 * @param createSlotOnStart Create logical decoding slot when the source starts (if it doesn't already exist...)
 * @param tablesToIgnore    Tables to ignore
 * @param columnsToIgnore   Columns to ignore
 * @param maxItems          Specifies how many rows are fetched in one batch
 * @param pollInterval      Duration between polls
 */
final class PostgreSQLInstance private (val connectionString: String,
                                        val slotName: String,
                                        val mode: Mode = Mode.Get,
                                        val createSlotOnStart: Boolean = true,
                                        val tablesToIgnore: List[String] = List(),
                                        val columnsToIgnore: Map[String, List[String]] = Map(),
                                        val maxItems: Int = 128,
                                        val pollInterval: FiniteDuration = 2000.milliseconds) {

  def withMode(mode: Mode): PostgreSQLInstance =
    copy(mode = mode)

  def withCreateSlotOnStart(createSlotOnStart: Boolean): PostgreSQLInstance =
    copy(createSlotOnStart = createSlotOnStart)

  def withTablesToIgnore(tablesToIgnore: List[String]): PostgreSQLInstance =
    copy(tablesToIgnore = tablesToIgnore)

  /**
   * Java API
   */
  def withTablesToIgnore(tablesToIgnore: JavaList[String]): PostgreSQLInstance =
    copy(tablesToIgnore = tablesToIgnore.asScala.toList)

  def withColumnsToIgnore(columnsToIgnore: Map[String, List[String]]): PostgreSQLInstance =
    copy(columnsToIgnore = columnsToIgnore)

  /**
   * Java API
   */
  def withColumnsToIgnore(columnsToIgnore: JavaMap[String, JavaList[String]]): PostgreSQLInstance =
    copy(columnsToIgnore = columnsToIgnore.asScala.mapValues(_.asScala).mapValues(_.toList).toMap)

  def withMaxItems(maxItems: Int): PostgreSQLInstance =
    copy(maxItems = maxItems)

  def withPollInterval(pollInterval: FiniteDuration): PostgreSQLInstance =
    copy(pollInterval = pollInterval)

  /**
   * Java API
   */
  def withPollInterval(pollInterval: JavaDuration): PostgreSQLInstance = {
    import scala.compat.java8.DurationConverters._
    copy(pollInterval = pollInterval.toScala)
  }

  private def copy(connectionString: String = connectionString,
                   slotName: String = slotName,
                   mode: Mode = mode,
                   createSlotOnStart: Boolean = createSlotOnStart,
                   tablesToIgnore: List[String] = tablesToIgnore,
                   columnsToIgnore: Map[String, List[String]] = columnsToIgnore,
                   maxItems: Int = maxItems,
                   pollInterval: FiniteDuration = pollInterval): PostgreSQLInstance =
    new PostgreSQLInstance(connectionString,
                           slotName,
                           mode,
                           createSlotOnStart,
                           tablesToIgnore,
                           columnsToIgnore,
                           maxItems,
                           pollInterval)

  override def toString: String =
    s"""
       |PostgreSQLInstance(
       | connectionString = $connectionString
       | slotName = $slotName
       | createSlotOnStart = $createSlotOnStart
       | tablesToIgnore = $tablesToIgnore
       | columnsToIgnore = $columnsToIgnore
       | mode = $Mode
       | maxItems = $maxItems
       | pollInterval = $pollInterval
       |)""".stripMargin

}

object PostgreSQLInstance {

  /**
   * Factory method for Scala.
   */
  def apply(connectionString: String, slotName: String): PostgreSQLInstance = {
    require(connectionString != null, "connectionString cannot be null")
    require(slotName.matches("[a-z0-9_]+"), "invalid replication slot name")
    new PostgreSQLInstance(connectionString, slotName)
  }

  /**
   * Java API
   *
   * Factory method for Java.
   */
  def create(connectionString: String, slotName: String): PostgreSQLInstance =
    PostgreSQLInstance(connectionString, slotName)
}
