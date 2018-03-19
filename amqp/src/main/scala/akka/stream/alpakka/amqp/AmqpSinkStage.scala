/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.amqp

import java.util.Optional

import akka.Done
import akka.stream.stage.{GraphStageLogic, GraphStageWithMaterializedValue, InHandler}
import akka.stream.{ActorAttributes, Attributes, Inlet, SinkShape}
import akka.util.ByteString
import com.rabbitmq.client.AMQP.BasicProperties
import com.rabbitmq.client._

import scala.compat.java8.OptionConverters
import scala.concurrent.{Future, Promise}

final case class OutgoingMessage(bytes: ByteString,
                                 immediate: Boolean,
                                 mandatory: Boolean,
                                 props: Option[BasicProperties] = None,
                                 routingKey: Option[String] = None) {

  /**
   * Java API
   */
  def this(bytes: ByteString,
           immediate: Boolean,
           mandatory: Boolean,
           props: Optional[BasicProperties],
           routingKey: Optional[String]) =
    this(bytes, immediate, mandatory, OptionConverters.toScala(props), OptionConverters.toScala(routingKey))
}

object AmqpSinkStage {

  private val defaultAttributes =
    Attributes.name("AmqpSink").and(ActorAttributes.dispatcher("akka.stream.default-blocking-io-dispatcher"))
}

/**
 * Connects to an AMQP server upon materialization and sends incoming messages to the server.
 * Each materialized sink will create one connection to the broker.
 */
final class AmqpSinkStage(settings: AmqpSinkSettings)
    extends GraphStageWithMaterializedValue[SinkShape[OutgoingMessage], Future[Done]] { stage =>
  import AmqpSinkStage._

  val in = Inlet[OutgoingMessage]("AmqpSink.in")

  override def shape: SinkShape[OutgoingMessage] = SinkShape.of(in)

  override protected def initialAttributes: Attributes = defaultAttributes

  override def createLogicAndMaterializedValue(inheritedAttributes: Attributes): (GraphStageLogic, Future[Done]) = {
    val promise = Promise[Done]()
    (new GraphStageLogic(shape) with AmqpConnectorLogic {
      override val settings = stage.settings
      private val exchange = settings.exchange.getOrElse("")
      private val routingKey = settings.routingKey.getOrElse("")

      override def whenConnected(): Unit = {
        val shutdownCallback = getAsyncCallback[ShutdownSignalException] { ex =>
          onFailure(ex)
        }
        channel.addShutdownListener(
          new ShutdownListener {
            override def shutdownCompleted(cause: ShutdownSignalException): Unit =
              shutdownCallback.invoke(cause)
          }
        )
        pull(in)
      }

      setHandler(
        in,
        new InHandler {

          override def onUpstreamFailure(ex: Throwable): Unit = {
            promise.failure(ex)
            super.onUpstreamFailure(ex)
          }

          override def onUpstreamFinish(): Unit = {
            promise.success(Done)
            super.onUpstreamFinish()
          }

          override def onPush(): Unit = {
            val elem = grab(in)
            channel.basicPublish(
              exchange,
              elem.routingKey.getOrElse(routingKey),
              elem.mandatory,
              elem.immediate,
              elem.props.orNull,
              elem.bytes.toArray
            )
            pull(in)
          }
        }
      )

      override def postStop(): Unit = {
        promise.tryFailure(new RuntimeException("stage stopped unexpectedly"))
        super.postStop()
      }

      override def onFailure(ex: Throwable): Unit = {
        promise.tryFailure(ex)
        super.onFailure(ex)
      }

    }, promise.future)
  }

  override def toString: String = "AmqpSink"
}

object AmqpReplyToSinkStage {

  private val defaultAttributes =
    Attributes.name("AmqpReplyToSink").and(ActorAttributes.dispatcher("akka.stream.default-blocking-io-dispatcher"))

}

/**
 * Connects to an AMQP server upon materialization and sends incoming messages to the server.
 * Each materialized sink will create one connection to the broker. This stage sends messages to
 * the queue named in the replyTo options of the message instead of from settings declared at construction.
 */
final class AmqpReplyToSinkStage(settings: AmqpReplyToSinkSettings)
    extends GraphStageWithMaterializedValue[SinkShape[OutgoingMessage], Future[Done]] { stage =>
  import AmqpReplyToSinkStage._

  val in = Inlet[OutgoingMessage]("AmqpReplyToSink.in")

  override def shape: SinkShape[OutgoingMessage] = SinkShape.of(in)

  override protected def initialAttributes: Attributes = defaultAttributes

  override def createLogicAndMaterializedValue(inheritedAttributes: Attributes): (GraphStageLogic, Future[Done]) = {
    val promise = Promise[Done]()
    (new GraphStageLogic(shape) with AmqpConnectorLogic {
      override val settings = stage.settings

      override def whenConnected(): Unit = {
        val shutdownCallback = getAsyncCallback[ShutdownSignalException] { ex =>
          onFailure(ex)
        }
        channel.addShutdownListener(
          new ShutdownListener {
            override def shutdownCompleted(cause: ShutdownSignalException): Unit =
              shutdownCallback.invoke(cause)
          }
        )
        pull(in)
      }

      override def postStop(): Unit = {
        promise.tryFailure(new RuntimeException("stage stopped unexpectedly"))
        super.postStop()
      }

      override def onFailure(ex: Throwable): Unit = {
        promise.tryFailure(ex)
        super.onFailure(ex)
      }

      setHandler(
        in,
        new InHandler {

          override def onUpstreamFailure(ex: Throwable): Unit = {
            promise.failure(ex)
            super.onUpstreamFailure(ex)
          }

          override def onUpstreamFinish(): Unit = {
            promise.success(Done)
            super.onUpstreamFinish()
          }

          override def onPush(): Unit = {
            val elem = grab(in)

            val replyTo = elem.props.map(_.getReplyTo)

            if (replyTo.isDefined) {
              channel.basicPublish(
                elem.routingKey.getOrElse(""),
                replyTo.get,
                elem.mandatory,
                elem.immediate,
                elem.props.orNull,
                elem.bytes.toArray
              )
            } else if (settings.failIfReplyToMissing) {
              onFailure(new RuntimeException("Reply-to header was not set"))
            }

            tryPull(in)
          }
        }
      )

    }, promise.future)
  }

  override def toString: String = "AmqpReplyToSink"
}
