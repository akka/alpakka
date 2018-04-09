/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.amqp

import akka.stream.stage.GraphStageLogic
import com.rabbitmq.client._

import scala.util.control.NonFatal

private[amqp] case class ConnectionWithShutdown(conn: Connection, onShutdown: ShutdownListener) {

  /**
   * @return a Channel with the shutdown listener pre-registered
   */
  def createChannel: Channel = {
    val ch = conn.createChannel()
    ch.addShutdownListener(onShutdown)
    ch
  }

  /**
   * Removes the shutdown listener from the connection
   */
  def shutdown(): Unit = conn.removeShutdownListener(onShutdown)
}

/**
 * Internal API
 */
private[amqp] trait AmqpConnectorLogic { this: GraphStageLogic =>

  private var connection: ConnectionWithShutdown = _
  protected var channel: Channel = _

  def settings: AmqpConnectorSettings
  def whenConnected(): Unit
  def onFailure(ex: Throwable): Unit = failStage(ex)

  final override def preStart(): Unit =
    try {
      val connShutdownCallback = getAsyncCallback[ShutdownSignalException] { ex =>
        if (!ex.isInitiatedByApplication) failStage(ex)
      }
      val shutdownListener = new ShutdownListener {
        override def shutdownCompleted(cause: ShutdownSignalException): Unit = connShutdownCallback.invoke(cause)
      }

      connection = {
        val c = settings.connectionProvider.get
        c.addShutdownListener(shutdownListener)
        ConnectionWithShutdown(c, shutdownListener)
      }

      channel = connection.createChannel

      import scala.collection.JavaConverters._

      settings.declarations.foreach {
        case QueueDeclaration(name, durable, exclusive, autoDelete, arguments) =>
          channel.queueDeclare(
            name,
            durable,
            exclusive,
            autoDelete,
            arguments.asJava
          )

        case BindingDeclaration(queue, exchange, routingKey, arguments) =>
          channel.queueBind(
            queue,
            exchange,
            routingKey.getOrElse(""),
            arguments.asJava
          )

        case ExchangeDeclaration(name, exchangeType, durable, autoDelete, internal, arguments) =>
          channel.exchangeDeclare(
            name,
            exchangeType,
            durable,
            autoDelete,
            internal,
            arguments.asJava
          )
      }

      whenConnected()
    } catch {
      case NonFatal(e) => onFailure(e)
    }

  /** remember to call if overriding! */
  override def postStop(): Unit = {
    if ((channel ne null) && channel.isOpen) channel.close()
    channel = null

    connection.shutdown
    settings.connectionProvider.release(connection.conn)
    connection = null
  }
}
