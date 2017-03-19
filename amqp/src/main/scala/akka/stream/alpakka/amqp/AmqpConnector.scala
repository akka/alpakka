/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.amqp

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
      case AmqpConnectionDetails(_,
                                 maybeCredentials,
                                 maybeVirtualHost,
                                 sslProtocol,
                                 requestedHeartbeat,
                                 connectionTimeout,
                                 handshakeTimeout,
                                 shutdownTimeout,
                                 networkRecoveryInterval,
                                 automaticRecoveryEnabled,
                                 topologyRecoveryEnabled,
                                 exceptionHandler) =>
        maybeCredentials.foreach { credentials =>
          factory.setUsername(credentials.username)
          factory.setPassword(credentials.password)
        }
        maybeVirtualHost.foreach(factory.setVirtualHost)
        sslProtocol.foreach(factory.useSslProtocol)
        requestedHeartbeat.foreach(factory.setRequestedHeartbeat)
        connectionTimeout.foreach(factory.setConnectionTimeout)
        handshakeTimeout.foreach(factory.setHandshakeTimeout)
        shutdownTimeout.foreach(factory.setShutdownTimeout)
        networkRecoveryInterval.foreach(factory.setNetworkRecoveryInterval)
        automaticRecoveryEnabled.foreach(factory.setAutomaticRecoveryEnabled)
        topologyRecoveryEnabled.foreach(factory.setTopologyRecoveryEnabled)
        exceptionHandler.foreach(factory.setExceptionHandler)
      case DefaultAmqpConnection => // leave it be as is
    }
    factory
  }

  def newConnection(factory: ConnectionFactory, settings: AmqpConnectionSettings): Connection = settings match {
    case a: AmqpConnectionDetails => {
      import scala.collection.JavaConverters._
      if (a.hostAndPortList.nonEmpty)
        factory.newConnection(a.hostAndPortList.map(hp => new Address(hp._1, hp._2)).asJava)
      else
        throw new IllegalArgumentException("You need to supply at least one host/port pair.")
    }
    case _ => factory.newConnection()
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
  def newConnection(factory: ConnectionFactory, settings: AmqpConnectionSettings): Connection
  def whenConnected(): Unit

  final override def preStart(): Unit = {
    val factory = connectionFactoryFrom(settings.connectionSettings)

    connection = newConnection(factory, settings.connectionSettings)
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
  }

  /** remember to call if overriding! */
  override def postStop(): Unit = {
    if ((channel ne null) && channel.isOpen) channel.close()
    channel = null
    if ((connection ne null) && connection.isOpen) connection.close()
    connection = null
  }
}
