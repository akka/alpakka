/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.amqp

import akka.Done
import akka.stream.stage.{GraphStage, GraphStageLogic, GraphStageWithMaterializedValue, InHandler}
import akka.stream.{ActorAttributes, Attributes, Inlet, SinkShape}
import akka.util.ByteString
import com.rabbitmq.client.AMQP.BasicProperties
import com.rabbitmq.client._

import scala.concurrent.{Future, Promise}

final case class OutgoingMessage(bytes: ByteString,
                                 immediate: Boolean,
                                 mandatory: Boolean,
                                 props: Option[BasicProperties])

object AmqpSinkStage {

  private val defaultAttributes =
    Attributes.name("AmqpSink").and(ActorAttributes.dispatcher("akka.stream.default-blocking-io-dispatcher"))
}

/**
 * Connects to an AMQP server upon materialization and sends incoming messages to the server.
 * Each materialized sink will create one connection to the broker.
 */
final class AmqpSinkStage(settings: AmqpSinkSettings)
    extends GraphStageWithMaterializedValue[SinkShape[OutgoingMessage], Future[Done]]
    with AmqpConnector { stage =>
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

      override def connectionFactoryFrom(settings: AmqpConnectionSettings) = stage.connectionFactoryFrom(settings)
      override def newConnection(factory: ConnectionFactory, settings: AmqpConnectionSettings) =
        stage.newConnection(factory, settings)

      override def whenConnected(): Unit = {
        val shutdownCallback = getAsyncCallback[ShutdownSignalException] { ex =>
          promise.failure(ex)
          failStage(ex)
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
              routingKey,
              elem.mandatory,
              elem.immediate,
              elem.props.orNull,
              elem.bytes.toArray
            )
            pull(in)
          }
        }
      )
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
    extends GraphStageWithMaterializedValue[SinkShape[OutgoingMessage], Future[Done]]
    with AmqpConnector { stage =>
  import AmqpReplyToSinkStage._

  val in = Inlet[OutgoingMessage]("AmqpReplyToSink.in")

  override def shape: SinkShape[OutgoingMessage] = SinkShape.of(in)

  override protected def initialAttributes: Attributes = defaultAttributes

  override def createLogicAndMaterializedValue(inheritedAttributes: Attributes): (GraphStageLogic, Future[Done]) = {
    val promise = Promise[Done]()
    (new GraphStageLogic(shape) with AmqpConnectorLogic {
      override val settings = stage.settings

      override def connectionFactoryFrom(settings: AmqpConnectionSettings) = stage.connectionFactoryFrom(settings)
      override def newConnection(factory: ConnectionFactory, settings: AmqpConnectionSettings): Connection =
        stage.newConnection(factory, settings)

      override def whenConnected(): Unit = {
        val shutdownCallback = getAsyncCallback[ShutdownSignalException] { ex =>
          promise.failure(ex)
          failStage(ex)
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
                "",
                replyTo.get,
                elem.mandatory,
                elem.immediate,
                elem.props.orNull,
                elem.bytes.toArray
              )
            } else if (settings.failIfReplyToMissing) {
              val ex = new RuntimeException("Reply-to header was not set")
              promise.failure(ex)
              failStage(ex)
            }

            tryPull(in)
          }
        }
      )
    }, promise.future)
  }

  override def toString: String = "AmqpReplyToSink"
}
