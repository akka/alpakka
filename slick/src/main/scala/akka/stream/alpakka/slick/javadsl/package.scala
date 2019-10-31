/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.slick.javadsl

import slick.basic.DatabaseConfig
import slick.jdbc.JdbcBackend
import slick.jdbc.JdbcProfile
import slick.jdbc.PositionedResult
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory

/**
 * Java API: Represents an "open" Slick database and its database (type) profile.
 *
 * <b>NOTE</b>: these databases need to be closed after creation to
 * avoid leaking database resources like active connection pools, etc.
 */
sealed abstract class SlickSession {
  val db: JdbcBackend#Database
  val profile: JdbcProfile

  /**
   * You are responsible for closing the database after use!!
   */
  def close(): Unit = db.close()
}

private[slick] abstract class SlickSessionFactory {
  protected final class SlickSessionConfigBackedImpl(val slick: DatabaseConfig[JdbcProfile]) extends SlickSession {
    val db: JdbcBackend#Database = slick.db
    val profile: JdbcProfile = slick.profile
  }
  protected final class SlickSessionDbAndProfileBackedImpl(val db: JdbcBackend#Database, val profile: JdbcProfile)
      extends SlickSession

  def forConfig(path: String): SlickSession = forConfig(path, ConfigFactory.load())
  def forConfig(config: Config): SlickSession = forConfig("", config)
  def forConfig(path: String, config: Config): SlickSession = forConfig(
    DatabaseConfig.forConfig[JdbcProfile](path, config)
  )
  def forConfig(databaseConfig: DatabaseConfig[JdbcProfile]): SlickSession =
    new SlickSessionConfigBackedImpl(databaseConfig)
}

/**
 * Java API: Methods for "opening" Slick databases for use.
 *
 * <b>NOTE</b>: databases created through these methods will need to be
 * closed after creation to avoid leaking database resources like active
 * connection pools, etc.
 */
object SlickSession extends SlickSessionFactory

/**
 * Java API: A class representing a slick resultset row, which is used
 *          in SlickSource to map result set rows back to Java objects.
 */
final class SlickRow private[javadsl] (delegate: PositionedResult) {
  final def nextBoolean(): java.lang.Boolean = delegate.nextBoolean()
  final def nextBigDecimal(): java.math.BigDecimal = delegate.nextBigDecimal().bigDecimal
  final def nextBlob(): java.sql.Blob = delegate.nextBlob()
  final def nextByte(): java.lang.Byte = delegate.nextByte()
  final def nextBytes(): Array[java.lang.Byte] = delegate.nextBytes().map(Byte.box(_))
  final def nextClob(): java.sql.Clob = delegate.nextClob()
  final def nextDate(): java.sql.Date = delegate.nextDate()
  final def nextDouble(): java.lang.Double = delegate.nextDouble()
  final def nextFloat(): java.lang.Float = delegate.nextFloat()
  final def nextInt(): java.lang.Integer = delegate.nextInt()
  final def nextLong(): java.lang.Long = delegate.nextLong()
  final def nextObject(): java.lang.Object = delegate.nextObject()
  final def nextShort(): java.lang.Short = delegate.nextShort()
  final def nextString(): java.lang.String = delegate.nextString()
  final def nextTime(): java.sql.Time = delegate.nextTime()
  final def nextTimestamp(): java.sql.Timestamp = delegate.nextTimestamp()
}
