/*
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.contrib.amqp

import akka.stream.stage.GraphStageLogic
import com.rabbitmq.client._

/**
 * Internal API
 */
private[amqp] trait AmqpConnector {

  def connectionFactoryFrom(settings: AmqpConnectionSettings): ConnectionFactory = {
    val factory = new ConnectionFactory
    settings match {
      case AmqpConnectionUri(uri) => factory.setUri(uri)
      case AmqpConnectionDetails(host, port, maybeCredentials, maybeVirtualHost) =>
        factory.setHost(host)
        factory.setPort(port)
        maybeCredentials.foreach { credentials =>
          factory.setUsername(credentials.username)
          factory.setPassword(credentials.password)
        }
        maybeVirtualHost.foreach(factory.setVirtualHost)

      case DefaultAmqpConnection => // leave it be as is
    }
    factory
  }
}

/**
 * Internal API
 */
private[amqp] trait AmqpConnectorLogic { this: GraphStageLogic =>

  private var connection: Connection = _
  protected var channel: Channel = _

  def settings: AmqpConnectorSettings
  def connectionFactoryFrom(settings: AmqpConnectionSettings): ConnectionFactory
  def whenConnected(): Unit

  final override def preStart(): Unit = {
    val factory = connectionFactoryFrom(settings.connectionSettings)

    connection = factory.newConnection()
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

      case BindingDeclaration(exchange, queue, routingKey, arguments) =>
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
  }

  /** remember to call if overriding! */
  override def postStop(): Unit = {
    if ((channel ne null) && channel.isOpen) channel.close()
    channel = null
    if ((connection ne null) && connection.isOpen) connection.close()
    connection = null
  }
}