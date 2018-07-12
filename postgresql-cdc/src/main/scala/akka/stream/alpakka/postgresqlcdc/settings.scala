/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.postgresqlcdc

// rename Java imports if the name clashes with the Scala name
import java.time.{Duration => JavaDuration}
import java.util.{List => JavaList, Map => JavaMap}

import scala.collection.JavaConverters._
import scala.concurrent.duration._
import scala.language.postfixOps

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
  def createGetMode(): Get = Get

  /**
   * Java API
   */
  def createPeekMode(): Peek = Peek

}

sealed abstract class Plugin {
  val name: String
}

object Plugins {

  sealed abstract class TestDecoding extends Plugin {
    override val name = "test_decoding"
  }

  case object TestDecoding extends TestDecoding

  // sealed abstract class Wal2Json extends Plugin /* WIP */

  /**
   * Java API
   */
  def createTestDecoding(): TestDecoding = TestDecoding

}

/** Settings for the PostgreSQL CDC source
 *
 * @param mode              Choose between "at most once delivery" / "at least once"
 * @param createSlotOnStart Create logical decoding slot when the source starts (if it doesn't already exist...)
 * @param plugin            Plugin to use. Only "test_decoding" supported right now.
 * @param columnsToIgnore   Columns to ignore
 * @param maxItems          Specifies how many rows are fetched in one batch
 * @param pollInterval      Duration between polls
 */
final class PgCdcSourceSettings private (val mode: Mode = Modes.Get,
                                         val createSlotOnStart: Boolean = true,
                                         val plugin: Plugin = Plugins.TestDecoding,
                                         val columnsToIgnore: Map[String, List[String]] = Map(),
                                         val maxItems: Int = 128,
                                         val pollInterval: FiniteDuration = 2000.milliseconds) {

  def withMode(mode: Mode): PgCdcSourceSettings =
    copy(mode = mode)

  def withCreateSlotOnStart(createSlotOnStart: Boolean): PgCdcSourceSettings =
    copy(createSlotOnStart = createSlotOnStart)

  def withPlugin(plugin: Plugin): PgCdcSourceSettings =
    copy(plugin = plugin)

  def withColumnsToIgnore(columnsToIgnore: Map[String, List[String]]): PgCdcSourceSettings =
    copy(columnsToIgnore = columnsToIgnore)

  /**
   * Java API
   */
  def withColumnsToIgnore(columnsToIgnore: JavaMap[String, JavaList[String]]): PgCdcSourceSettings =
    copy(columnsToIgnore = columnsToIgnore.asScala.mapValues(_.asScala).mapValues(_.toList).toMap)

  def withMaxItems(maxItems: Int): PgCdcSourceSettings =
    copy(maxItems = maxItems)

  def withPollInterval(pollInterval: FiniteDuration): PgCdcSourceSettings =
    copy(pollInterval = pollInterval)

  /**
   * Java API
   */
  def withPollInterval(pollInterval: JavaDuration): PgCdcSourceSettings = {
    import scala.compat.java8.DurationConverters._
    copy(pollInterval = pollInterval.toScala)
  }

  private def copy(mode: Mode = mode,
                   createSlotOnStart: Boolean = createSlotOnStart,
                   plugin: Plugin = plugin,
                   columnsToIgnore: Map[String, List[String]] = columnsToIgnore,
                   maxItems: Int = maxItems,
                   pollInterval: FiniteDuration = pollInterval): PgCdcSourceSettings =
    new PgCdcSourceSettings(mode, createSlotOnStart, plugin, columnsToIgnore, maxItems, pollInterval)

  override def toString: String =
    s"""
       |SourceSettings(
       |  mode = $mode
       |  createSlotOnStart = $createSlotOnStart
       |  plugin = $plugin
       |  columnsToIgnore = $columnsToIgnore
       |  maxItems = $maxItems
       |  pollInterval = $pollInterval
       |)""".stripMargin

}

object PgCdcSourceSettings {

  /**
   * Factory method for Scala.
   */
  def apply(): PgCdcSourceSettings =
    new PgCdcSourceSettings()

  /**
   * Java API
   *
   * Factory method for Java.
   */
  def create(): PgCdcSourceSettings =
    PgCdcSourceSettings()
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

final class PgCdcAckSinkSettings private (val maxItems: Int = 16,
                                          val maxItemsWait: FiniteDuration = 3000 milliseconds) {

  override def toString =
    s"""AckSinkSettings(
       |  maxItems=$maxItems,
       |  maxItemsWait=$maxItemsWait
       )""".stripMargin

  def withMaxItemsWait(maxItemsWait: FiniteDuration): PgCdcAckSinkSettings =
    copy(maxItemsWait = maxItemsWait)

  /**
   * Java API
   */
  def withMaxItemsWait(maxItemsWait: JavaDuration): PgCdcAckSinkSettings = {
    import scala.compat.java8.DurationConverters._
    copy(maxItemsWait = maxItemsWait.toScala)
  }

  def withMaxItems(maxItems: Int): PgCdcAckSinkSettings = copy(maxItems = maxItems)

  private def copy(maxItems: Int = maxItems, maxItemsWait: FiniteDuration = maxItemsWait): PgCdcAckSinkSettings =
    new PgCdcAckSinkSettings(maxItems, maxItemsWait)

}

object PgCdcAckSinkSettings {

  /**
   * Factory method for Scala.
   */
  def apply() = new PgCdcAckSinkSettings()

  /**
   * Java API
   *
   * Factory method for Java.
   */
  def create(): PgCdcAckSinkSettings = PgCdcAckSinkSettings()

}
