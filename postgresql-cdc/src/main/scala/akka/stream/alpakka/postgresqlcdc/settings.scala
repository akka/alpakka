/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.postgresqlcdc

// rename Java imports if the name clashes with the Scala name
import java.time.{Duration => JavaDuration}
import java.util.{List => JavaList, Map => JavaMap}

import scala.collection.JavaConverters._
import scala.concurrent.duration._

sealed abstract class Mode

object Modes {

  /**
   * We make singleton objects extend an abstract class with the same name.
   * This makes it possible to refer to the object type without `.type`.
   */
  // at most once delivery
  sealed abstract class Get extends Mode
  case object Get extends Get

  // at least once delivery
  sealed abstract class Peek extends Mode
  case object Peek extends Peek

  /**
   * Java API
   */
  def chooseGetMode(): Get = Get

  /**
   * Java API
   */
  def choosePeekMode(): Peek = Peek

}

/** Settings for PostgreSQL CDC
 *
 * @param mode              Choose between "at most once delivery" / "at least once"
 * @param createSlotOnStart Create logical decoding slot when the source starts (if it doesn't already exist...)
 * @param tablesToIgnore    Tables to ignore
 * @param columnsToIgnore   Columns to ignore
 * @param maxItems          Specifies how many rows are fetched in one batch
 * @param pollInterval      Duration between polls
 */
final class ChangeDataCaptureSettings private (val mode: Mode = Modes.Get,
                                               val createSlotOnStart: Boolean = true,
                                               val tablesToIgnore: List[String] = List(),
                                               val columnsToIgnore: Map[String, List[String]] = Map(),
                                               val maxItems: Int = 128,
                                               val pollInterval: FiniteDuration = 2000.milliseconds) {

  def withMode(mode: Mode): ChangeDataCaptureSettings =
    copy(mode = mode)

  def withCreateSlotOnStart(createSlotOnStart: Boolean): ChangeDataCaptureSettings =
    copy(createSlotOnStart = createSlotOnStart)

  def withTablesToIgnore(tablesToIgnore: List[String]): ChangeDataCaptureSettings =
    copy(tablesToIgnore = tablesToIgnore)

  /**
   * Java API
   */
  def withTablesToIgnore(tablesToIgnore: JavaList[String]): ChangeDataCaptureSettings =
    copy(tablesToIgnore = tablesToIgnore.asScala.toList)

  def withColumnsToIgnore(columnsToIgnore: Map[String, List[String]]): ChangeDataCaptureSettings =
    copy(columnsToIgnore = columnsToIgnore)

  /**
   * Java API
   */
  def withColumnsToIgnore(columnsToIgnore: JavaMap[String, JavaList[String]]): ChangeDataCaptureSettings =
    copy(columnsToIgnore = columnsToIgnore.asScala.mapValues(_.asScala).mapValues(_.toList).toMap)

  def withMaxItems(maxItems: Int): ChangeDataCaptureSettings =
    copy(maxItems = maxItems)

  def withPollInterval(pollInterval: FiniteDuration): ChangeDataCaptureSettings =
    copy(pollInterval = pollInterval)

  /**
   * Java API
   */
  def withPollInterval(pollInterval: JavaDuration): ChangeDataCaptureSettings = {
    import scala.compat.java8.DurationConverters._
    copy(pollInterval = pollInterval.toScala)
  }

  private def copy(mode: Mode = mode,
                   createSlotOnStart: Boolean = createSlotOnStart,
                   tablesToIgnore: List[String] = tablesToIgnore,
                   columnsToIgnore: Map[String, List[String]] = columnsToIgnore,
                   maxItems: Int = maxItems,
                   pollInterval: FiniteDuration = pollInterval): ChangeDataCaptureSettings =
    new ChangeDataCaptureSettings(mode, createSlotOnStart, tablesToIgnore, columnsToIgnore, maxItems, pollInterval)

  override def toString: String =
    s"""
       |ChangeDataCaptureSettings(
       |  createSlotOnStart = $createSlotOnStart
       |  tablesToIgnore = $tablesToIgnore
       |  columnsToIgnore = $columnsToIgnore
       |  mode = $mode
       |  maxItems = $maxItems
       |  pollInterval = $pollInterval
       |)""".stripMargin

}

object ChangeDataCaptureSettings {

  /**
   * Factory method for Scala.
   */
  def apply(): ChangeDataCaptureSettings =
    new ChangeDataCaptureSettings()

  /**
   * Java API
   *
   * Factory method for Java.
   */
  def create(): ChangeDataCaptureSettings =
    ChangeDataCaptureSettings()
}

/**
 * PostgreSQL connection settings
 *
 * @param jdbcConnectionString JDBC connection string
 * @param slotName             Name of logical slot
 */
final class PostgreSQLInstance private (val jdbcConnectionString: String, val slotName: String) {

  // no reason to have withXxxx(...) since both jdbcConnectionString and slotName are required arguments

  override def toString =
    s"""PostgreSQLInstance(
       |  jdbcConnectionString = $jdbcConnectionString,
       |  slotName=$slotName
       |)""".stripMargin
}

object PostgreSQLInstance {

  /**
   * Factory method for Scala.
   */
  def apply(jdbcConnectionString: String, slotName: String): PostgreSQLInstance =
    new PostgreSQLInstance(jdbcConnectionString, slotName)

  /**
   * Java API
   *
   * Factory method for Java.
   */
  def create(jdbcConnectionString: String, slotName: String): PostgreSQLInstance =
    PostgreSQLInstance(jdbcConnectionString, slotName)

}
