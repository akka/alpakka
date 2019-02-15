/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.amqp.impl

import akka.Done
import akka.annotation.InternalApi
import akka.stream.alpakka.amqp._
import akka.stream.alpakka.amqp.scaladsl.CommittableIncomingMessage
import akka.stream.stage.{GraphStage, GraphStageLogic, OutHandler, StageLogging}
import akka.stream.{Attributes, Outlet, SourceShape}
import akka.util.ByteString
import com.rabbitmq.client.AMQP.BasicProperties
import com.rabbitmq.client.{DefaultConsumer, Envelope, ShutdownSignalException}

import scala.collection.mutable
import scala.concurrent.{Future, Promise}
import scala.util.Success

private final case class AckArguments(deliveryTag: Long, multiple: Boolean, promise: Promise[Done])
private final case class NackArguments(deliveryTag: Long, multiple: Boolean, requeue: Boolean, promise: Promise[Done])

/**
 * Connects to an AMQP server upon materialization and consumes messages from it emitting them
 * into the stream. Each materialized source will create one connection to the broker.
 * As soon as an `IncomingMessage` is sent downstream, an ack for it is sent to the broker.
 *
 * @param bufferSize The max number of elements to prefetch and buffer at any given time.
 */
@InternalApi
private[amqp] final class AmqpSourceStage(settings: AmqpSourceSettings, bufferSize: Int)
    extends GraphStage[SourceShape[CommittableIncomingMessage]] { stage =>

  private val out = Outlet[CommittableIncomingMessage]("AmqpSource.out")

  override val shape: SourceShape[CommittableIncomingMessage] = SourceShape.of(out)

  override protected def initialAttributes: Attributes = Attributes.name("AmqpSource")

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) with AmqpConnectorLogic with StageLogging {

      override val settings: AmqpSourceSettings = stage.settings

      private val queue = mutable.Queue[CommittableIncomingMessage]()
      private var finishWithOutstandingAcks = false
      private var unackedMessages = 0

      override def whenConnected(): Unit = {
        import scala.collection.JavaConverters._
        channel.basicQos(bufferSize)
        val consumerCallback = getAsyncCallback(handleDelivery)

        val ackCallback = getAsyncCallback[AckArguments] {
          case AckArguments(deliveryTag, multiple, promise) =>
            try {
              channel.basicAck(deliveryTag, multiple)
              unackedMessages -= 1
              if (unackedMessages == 0 && isClosed(out)) completeStage()
              promise.complete(Success(Done))
            } catch {
              case e: Throwable => promise.failure(e)
            }
        }

        val nackCallback = getAsyncCallback[NackArguments] {
          case NackArguments(deliveryTag, multiple, requeue, promise) =>
            try {
              channel.basicNack(deliveryTag, multiple, requeue)
              unackedMessages -= 1
              if (unackedMessages == 0 && isClosed(out)) completeStage()
              promise.complete(Success(Done))
            } catch {
              case e: Throwable => promise.failure(e)
            }
        }

        val amqpSourceConsumer = new DefaultConsumer(channel) {
          override def handleDelivery(consumerTag: String,
                                      envelope: Envelope,
                                      properties: BasicProperties,
                                      body: Array[Byte]): Unit =
            consumerCallback.invoke(
              new CommittableIncomingMessage {
                override val message = IncomingMessage(ByteString(body), envelope, properties)

                override def ack(multiple: Boolean): Future[Done] = {
                  val promise = Promise[Done]()
                  ackCallback.invoke(AckArguments(message.envelope.getDeliveryTag, multiple, promise))
                  promise.future
                }

                override def nack(multiple: Boolean, requeue: Boolean): Future[Done] = {
                  val promise = Promise[Done]()
                  nackCallback.invoke(NackArguments(message.envelope.getDeliveryTag, multiple, requeue, promise))
                  promise.future
                }
              }
            )

          override def handleCancel(consumerTag: String): Unit =
            // non consumer initiated cancel, for example happens when the queue has been deleted.
            shutdownCallback.invoke(
              new RuntimeException(s"Consumer with consumerTag $consumerTag shut down unexpectedly")
            )

          override def handleShutdownSignal(consumerTag: String, sig: ShutdownSignalException): Unit =
            // "Called when either the channel or the underlying connection has been shut down."
            shutdownCallback.invoke(
              new RuntimeException(s"Consumer with consumerTag $consumerTag shut down unexpectedly", sig)
            )
        }

        def setupNamedQueue(settings: NamedQueueSourceSettings): Unit =
          channel.basicConsume(
            settings.queue,
            !settings.ackRequired,
            settings.consumerTag, // consumer tag
            settings.noLocal,
            settings.exclusive,
            settings.arguments.asJava,
            amqpSourceConsumer
          )

        def setupTemporaryQueue(settings: TemporaryQueueSourceSettings): Unit = {
          // this is a weird case that required dynamic declaration, the queue name is not known
          // up front, it is only useful for sources, so that's why it's not placed in the AmqpConnectorLogic
          val queueName = channel.queueDeclare().getQueue
          channel.queueBind(queueName, settings.exchange, settings.routingKey.getOrElse(""))
          channel.basicConsume(
            queueName,
            amqpSourceConsumer
          )
        }

        settings match {
          case settings: NamedQueueSourceSettings =>
            finishWithOutstandingAcks = !settings.ackRequired
            setupNamedQueue(settings)
          case settings: TemporaryQueueSourceSettings =>
            setupTemporaryQueue(settings)
        }
      }

      def handleDelivery(message: CommittableIncomingMessage): Unit =
        if (isAvailable(out)) {
          pushMessage(message)
        } else if (queue.size + 1 > bufferSize) {
          onFailure(new RuntimeException(s"Reached maximum buffer size $bufferSize"))
        } else {
          queue.enqueue(message)
        }

      setHandler(
        out,
        new OutHandler {
          override def onPull(): Unit =
            if (queue.nonEmpty) {
              pushMessage(queue.dequeue())
            }

          override def onDownstreamFinish(): Unit = {
            if (finishWithOutstandingAcks || unackedMessages == 0) super.onDownstreamFinish()
            else {
              setKeepGoing(true)
              log.debug("Awaiting {} acks before finishing.", unackedMessages)
            }
          }
        }
      )

      def pushMessage(message: CommittableIncomingMessage): Unit = {
        push(out, message)
        unackedMessages += 1
      }
    }

}
