/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.amqp

import java.util.concurrent.atomic.AtomicReference

import com.rabbitmq.client.Connection

import scala.compat.java8.FunctionConverters._

/**
 * Only for internal implementations
 */
sealed trait AmqpConnectionProvider {
  def settings: AmqpConnectionSettings
  def get: Connection
  def release(connection: Connection): Unit
}

final object DefaultAmqpConnectionProvider {

  /**
   * Java API
   */
  def create(settings: AmqpConnectionSettings): DefaultAmqpConnectionProvider =
    DefaultAmqpConnectionProvider(settings: AmqpConnectionSettings)
}

final case class DefaultAmqpConnectionProvider(settings: AmqpConnectionSettings) extends AmqpConnectionProvider {
  override def get = settings.getConnection
  override def release(connection: Connection) = if (connection.isOpen) connection.close()
}

final object ReusableAmqpConnectionProvider {

  /**
   * Java API
   */
  def create(settings: AmqpConnectionSettings): ReusableAmqpConnectionProvider =
    ReusableAmqpConnectionProvider(settings: AmqpConnectionSettings)

  /**
   * Java API
   */
  def create(settings: AmqpConnectionSettings, automaticRelease: Boolean): ReusableAmqpConnectionProvider =
    ReusableAmqpConnectionProvider(settings: AmqpConnectionSettings, automaticRelease)
}

final case class ReusableAmqpConnectionProvider(settings: AmqpConnectionSettings, automaticRelease: Boolean = true)
    extends AmqpConnectionProvider {
  var clients = 0
  private val cachedConnectionRef = new AtomicReference[Option[Connection]](None)

  override def get =
    cachedConnectionRef
      .updateAndGet(
        asJavaUnaryOperator(
          cachedConnection =>
            cachedConnection match {
              case Some(connection) => {
                if (connection.isOpen) {
                  clients += 1
                  cachedConnection
                } else {
                  clients = 1
                  Some(settings.getConnection)
                }
              }
              case None => {
                clients = 1
                Some(settings.getConnection)
              }
          }
        )
      )
      .get

  override def release(connection: Connection) =
    cachedConnectionRef
      .updateAndGet(
        asJavaUnaryOperator(
          cachedConnection =>
            cachedConnection match {
              case Some(conn) =>
                if (conn != connection)
                  throw new IllegalArgumentException("Can't release a connection that's not owned by this provider")
                if (!automaticRelease) {
                  clients = 0
                  if (connection.isOpen) connection.close()
                  None
                } else {
                  clients -= 1
                  if (clients == 0) {
                    if (connection.isOpen) connection.close()
                    None
                  }
                  cachedConnection
                }
              case None => None
          }
        )
      )
}
