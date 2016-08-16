/*
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.contrib.amqp

import akka.NotUsed
import akka.stream.scaladsl.Sink
import akka.stream.stage.{ GraphStage, GraphStageLogic, InHandler }
import akka.stream.{ ActorAttributes, Attributes, Inlet, SinkShape }
import akka.util.ByteString
import com.rabbitmq.client.AMQP.BasicProperties
import com.rabbitmq.client._

final case class OutgoingMessage(bytes: ByteString, immediate: Boolean, mandatory: Boolean, props: Option[BasicProperties])

object AmqpSink {

  def simple(settings: AmqpSinkSettings): Sink[ByteString, NotUsed] =
    apply(settings).contramap[ByteString](bytes => OutgoingMessage(bytes, false, false, None))

  /**
   * Scala API:
   */
  def apply(settings: AmqpSinkSettings): Sink[OutgoingMessage, NotUsed] =
    Sink.fromGraph(new AmqpSink(settings))

  /**
   * Java API:
   */
  def create(settings: AmqpSinkSettings): akka.stream.javadsl.Sink[OutgoingMessage, NotUsed] =
    akka.stream.javadsl.Sink.fromGraph(new AmqpSink(settings))

  /**
   * Internal API
   */
  private val defaultAttributes = Attributes.name("AmsqpSink")
    .and(ActorAttributes.dispatcher("akka.stream.default-blocking-io-dispatcher"))
}

final class AmqpSink(settings: AmqpSinkSettings) extends GraphStage[SinkShape[OutgoingMessage]] with AmqpConnector { stage =>
  import AmqpSink._

  val in = Inlet[OutgoingMessage]("AmqpSink.in")

  override def shape: SinkShape[OutgoingMessage] = SinkShape.of(in)

  override protected def initialAttributes: Attributes = defaultAttributes

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) with AmqpConnectorLogic {
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
