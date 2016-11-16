/*
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.amqp

import akka.stream.stage.{ GraphStage, GraphStageLogic, InHandler }
import akka.stream.{ ActorAttributes, Attributes, Inlet, SinkShape }
import akka.util.ByteString
import com.rabbitmq.client.AMQP.BasicProperties
import com.rabbitmq.client._

final case class OutgoingMessage(bytes: ByteString,
                                 immediate: Boolean,
                                 mandatory: Boolean,
                                 props: Option[BasicProperties])

object AmqpSinkStage {

  /**
   * Internal API
   */
  private val defaultAttributes =
    Attributes.name("AmsqpSink").and(ActorAttributes.dispatcher("akka.stream.default-blocking-io-dispatcher"))
}

/**
 * Connects to an AMQP server upon materialization and sends incoming messages to the server.
 * Each materialized sink will create one connection to the broker.
 */
final class AmqpSinkStage(settings: AmqpSinkSettings)
    extends GraphStage[SinkShape[OutgoingMessage]]
    with AmqpConnector { stage =>
  import AmqpSinkStage._

  val in = Inlet[OutgoingMessage]("AmqpSink.in")

  override def shape: SinkShape[OutgoingMessage] = SinkShape.of(in)

  override protected def initialAttributes: Attributes = defaultAttributes

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) with AmqpConnectorLogic {
      override val settings = stage.settings
      private val exchange = settings.exchange.getOrElse("")
      private val routingKey = settings.routingKey.getOrElse("")

      override def connectionFactoryFrom(settings: AmqpConnectionSettings) = stage.connectionFactoryFrom(settings)

      override def whenConnected(): Unit = {
        val shutdownCallback = getAsyncCallback[ShutdownSignalException] { ex =>
          failStage(ex)
        }
        channel.addShutdownListener(new ShutdownListener {
          override def shutdownCompleted(cause: ShutdownSignalException): Unit = {
            shutdownCallback.invoke(cause)
          }
        })
        pull(in)
      }

      setHandler(in, new InHandler {
        override def onPush(): Unit = {
          val elem = grab(in)
          channel.basicPublish(
            exchange,
            routingKey,
            elem.mandatory,
            elem.immediate,
            elem.props.orNull,
            elem.bytes.toArray
          )
          pull(in)
        }
      })

    }

  override def toString: String = "AmqpSink"
}
