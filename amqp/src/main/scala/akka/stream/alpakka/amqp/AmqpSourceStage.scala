/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.amqp

import akka.Done
import akka.stream._
import akka.stream.alpakka.amqp.scaladsl.CommittableIncomingMessage
import akka.stream.stage._
import akka.util.ByteString
import com.rabbitmq.client.AMQP.BasicProperties
import com.rabbitmq.client._

import scala.collection.mutable
import scala.concurrent.Promise
import scala.util.Try

final case class IncomingMessage(bytes: ByteString, envelope: Envelope, properties: BasicProperties)

trait CommitCallback
final case class AckArguments(deliveryTag: Long, multiple: Boolean, promise: Promise[Done]) extends CommitCallback
final case class NackArguments(deliveryTag: Long, multiple: Boolean, requeue: Boolean, promise: Promise[Done])
    extends CommitCallback

object AmqpSourceStage {

  private val defaultAttributes = Attributes.name("AmqpSource")

}

/**
 * Connects to an AMQP server upon materialization and consumes messages from it emitting them
 * into the stream. Each materialized source will create one connection to the broker.
 * As soon as an `IncomingMessage` is sent downstream, an ack for it is sent to the broker.
 *
 * @param bufferSize The max number of elements to prefetch and buffer at any given time.
 */
final class AmqpSourceStage(settings: AmqpSourceSettings, bufferSize: Int)
    extends GraphStage[SourceShape[CommittableIncomingMessage]] { stage =>

  val out = Outlet[CommittableIncomingMessage]("AmqpSource.out")

  override val shape: SourceShape[CommittableIncomingMessage] = SourceShape.of(out)

  override protected def initialAttributes: Attributes = AmqpSourceStage.defaultAttributes

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) with AmqpConnectorLogic {

      override val settings = stage.settings

      private val queue = mutable.Queue[CommittableIncomingMessage]()
      private var unackedMessages = 0

      override def whenConnected(): Unit = {
        import scala.collection.JavaConverters._
        // we have only one consumer per connection so global is ok
        channel.basicQos(bufferSize, true)
        val consumerCallback = getAsyncCallback(handleDelivery)
        val shutdownCallback = getAsyncCallback[Option[ShutdownSignalException]] {
          case Some(ex) => onFailure(ex)
          case None => if (unackedMessages == 0) completeStage()
        }

        val commitCallback = getAsyncCallback[CommitCallback] {
          case AckArguments(deliveryTag, multiple, promise) => {
            try {
              channel.basicAck(deliveryTag, multiple)
              unackedMessages -= 1
              if (unackedMessages == 0 && isClosed(out)) completeStage()
              promise.complete(Try(Done))
            } catch {
              case e: Throwable => promise.failure(e)
            }
          }
          case NackArguments(deliveryTag, multiple, requeue, promise) => {
            try {
              channel.basicNack(deliveryTag, multiple, requeue)
              unackedMessages -= 1
              if (unackedMessages == 0 && isClosed(out)) completeStage()
              promise.complete(Try(Done))
            } catch {
              case e: Throwable => promise.failure(e)
            }
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

                override def ack(multiple: Boolean) = {
                  val promise = Promise[Done]()
                  commitCallback.invoke(AckArguments(message.envelope.getDeliveryTag, multiple, promise))
                  promise.future
                }

                override def nack(multiple: Boolean, requeue: Boolean) = {
                  val promise = Promise[Done]()
                  commitCallback.invoke(NackArguments(message.envelope.getDeliveryTag, multiple, requeue, promise))
                  promise.future
                }
              }
            )

          override def handleCancel(consumerTag: String): Unit =
            // non consumer initiated cancel, for example happens when the queue has been deleted.
            shutdownCallback.invoke(None)

          override def handleShutdownSignal(consumerTag: String, sig: ShutdownSignalException): Unit =
            // "Called when either the channel or the underlying connection has been shut down."
            shutdownCallback.invoke(Option(sig))
        }

        def setupNamedQueue(settings: NamedQueueSourceSettings): Unit =
          channel.basicConsume(
            settings.queue,
            false, // never auto-ack
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
          case settings: NamedQueueSourceSettings => setupNamedQueue(settings)
          case settings: TemporaryQueueSourceSettings => setupTemporaryQueue(settings)
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
            setKeepGoing(true)
            if (unackedMessages == 0) super.onDownstreamFinish()
          }
        }
      )

      def pushMessage(message: CommittableIncomingMessage): Unit = {
        push(out, message)
        unackedMessages += 1
      }
    }

}
