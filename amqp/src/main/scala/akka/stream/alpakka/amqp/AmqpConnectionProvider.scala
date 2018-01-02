/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.amqp

import java.util.concurrent.atomic.AtomicReference

import akka.annotation.DoNotInherit
import com.rabbitmq.client.Connection

import scala.annotation.tailrec

/**
 * Only for internal implementations
 */
@DoNotInherit
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

  private sealed trait State
  private case object Empty extends State
  private case object Connecting extends State
  private final case class Connected(connection: Connection, clients: Int) extends State
  private case object Closing extends State
}

final case class ReusableAmqpConnectionProvider(settings: AmqpConnectionSettings, automaticRelease: Boolean = true)
    extends AmqpConnectionProvider {

  import akka.stream.alpakka.amqp.ReusableAmqpConnectionProvider._
  private val state = new AtomicReference[State](Empty)

  @tailrec
  override def get: Connection = state.get match {
    case Empty =>
      if (state.compareAndSet(Empty, Connecting)) {
        val connection = settings.getConnection
        state.compareAndSet(Connecting, Connected(connection, 0))
        connection
      } else get
    case c @ Connected(connection, clients) =>
      if (state.compareAndSet(c, Connected(connection, clients + 1))) connection
      else get
    case _ => get
  }

  @tailrec
  override def release(connection: Connection) = state.get match {
    case c @ Connected(cachedConnection, clients) =>
      if (state.compareAndSet(c, Closing)) {
        if (cachedConnection != connection)
          throw new IllegalArgumentException("Can't release a connection that's not owned by this provider")
        else if (clients == 0 || !automaticRelease) {
          connection.close()
          state.compareAndSet(Closing, Empty)
        } else {
          state.compareAndSet(Closing, Connected(cachedConnection, clients - 1))
        }
      } else release(connection)
    case _ => release(connection)
  }
}
