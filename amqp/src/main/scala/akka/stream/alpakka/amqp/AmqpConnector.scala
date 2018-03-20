/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.amqp

import akka.stream.stage.GraphStageLogic
import com.rabbitmq.client._

import scala.util.control.NonFatal

/**
 * Internal API
 */
private[amqp] trait AmqpConnectorLogic { this: GraphStageLogic =>

  private var connection: Connection = _
  protected var channel: Channel = _

  def settings: AmqpConnectorSettings
  def whenConnected(): Unit
  def onFailure(ex: Throwable): Unit = failStage(ex)

  final override def preStart(): Unit =
    try {
      connection = settings.connectionProvider.get
      channel = connection.createChannel()

      val connShutdownCallback = getAsyncCallback[ShutdownSignalException] { ex =>
        if (!ex.isInitiatedByApplication) failStage(ex)
      }
      val shutdownListener = new ShutdownListener {
        override def shutdownCompleted(cause: ShutdownSignalException): Unit = connShutdownCallback.invoke(cause)
      }
      connection.addShutdownListener(shutdownListener)
      channel.addShutdownListener(shutdownListener)

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
    settings.connectionProvider.release(connection)
    connection = null
  }
}
