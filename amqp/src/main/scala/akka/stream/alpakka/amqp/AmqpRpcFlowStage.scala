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
import scala.concurrent.{Future, Promise}
import scala.util.Try

object AmqpRpcFlowStage {

  private val defaultAttributes =
    Attributes.name("AmqpRpcFlow").and(ActorAttributes.dispatcher("akka.stream.default-blocking-io-dispatcher"))
}

/**
 * This stage materializes to a Future[String], which is the name of the private exclusive queue used for RPC communication
 *
 * @param responsesPerMessage The number of responses that should be expected for each message placed on the queue. This
 *                            can be overridden per message by including `expectedReplies` in the the header of the [[OutgoingMessage]]
 */
final class AmqpRpcFlowStage(settings: AmqpSinkSettings, bufferSize: Int, responsesPerMessage: Int = 1)
    extends GraphStageWithMaterializedValue[FlowShape[OutgoingMessage, CommittableIncomingMessage], Future[String]] {
  stage =>

  val in = Inlet[OutgoingMessage]("AmqpRpcFlow.in")
  val out = Outlet[CommittableIncomingMessage]("AmqpRpcFlow.out")

  override def shape: FlowShape[OutgoingMessage, CommittableIncomingMessage] = FlowShape.of(in, out)

  override protected def initialAttributes: Attributes = AmqpRpcFlowStage.defaultAttributes

  override def createLogicAndMaterializedValue(inheritedAttributes: Attributes): (GraphStageLogic, Future[String]) = {
    val promise = Promise[String]()
    (new GraphStageLogic(shape) with AmqpConnectorLogic {

      override val settings = stage.settings
      private val exchange = settings.exchange.getOrElse("")
      private val routingKey = settings.routingKey.getOrElse("")
      private val queue = mutable.Queue[CommittableIncomingMessage]()
      private var queueName: String = _
      private var unackedMessages = 0
      private var outstandingMessages = 0

      override def whenConnected(): Unit = {
        import scala.collection.JavaConverters._
        val shutdownCallback = getAsyncCallback[(String, Option[ShutdownSignalException])] {
          case (consumerTag, Some(e)) =>
            onFailure(
              new RuntimeException(s"Consumer $queueName with consumerTag $consumerTag shut down unexpectedly", e)
            )
          case (consumerTag, None) =>
            onFailure(new RuntimeException(s"Consumer $queueName with consumerTag $consumerTag shut down unexpectedly"))
        }

        pull(in)

        // we have only one consumer per connection so global is ok
        channel.basicQos(bufferSize, true)
        val consumerCallback = getAsyncCallback(handleDelivery)

        val commitCallback = getAsyncCallback[CommitCallback] {
          case AckArguments(deliveryTag, multiple, promise) => {
            try {
              channel.basicAck(deliveryTag, multiple)
              unackedMessages -= 1
              if (unackedMessages == 0 && (isClosed(out) || (isClosed(in) && queue.isEmpty && outstandingMessages == 0)))
                completeStage()
              promise.complete(Try(Done))
            } catch {
              case e: Throwable => promise.failure(e)
            }
          }
          case NackArguments(deliveryTag, multiple, requeue, promise) => {
            try {
              channel.basicNack(deliveryTag, multiple, requeue)
              unackedMessages -= 1
              if (unackedMessages == 0 && (isClosed(out) || (isClosed(in) && queue.isEmpty && outstandingMessages == 0)))
                completeStage()
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
            shutdownCallback.invoke((consumerTag, None))

          override def handleShutdownSignal(consumerTag: String, sig: ShutdownSignalException): Unit =
            // "Called when either the channel or the underlying connection has been shut down."
            shutdownCallback.invoke((consumerTag, Option(sig)))
        }

        // Create an exclusive queue with a randomly generated name for use as the replyTo portion of RPC
        queueName = channel
          .queueDeclare(
            "",
            false,
            true,
            true,
            Map.empty[String, AnyRef].asJava
          )
          .getQueue

        channel.basicConsume(
          queueName,
          amqpSourceConsumer
        )
        promise.success(queueName)
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
        outstandingMessages -= 1
      }

      setHandler(
        in,
        new InHandler {
          // We don't want to finish since we're still waiting
          // on incoming messages from rabbit. However, if we
          // haven't processed a message yet, we do want to complete
          // so that we don't hang.
          override def onUpstreamFinish(): Unit = {
            setKeepGoing(true)
            if (queue.isEmpty && outstandingMessages == 0 && unackedMessages == 0) super.onUpstreamFinish()
          }

          override def onUpstreamFailure(ex: Throwable): Unit = {
            setKeepGoing(true)
            if (queue.isEmpty && outstandingMessages == 0 && unackedMessages == 0)
              super.onUpstreamFailure(ex)
          }

          override def onPush(): Unit = {
            val elem = grab(in)
            val props = elem.props.getOrElse(new BasicProperties()).builder.replyTo(queueName).build()
            channel.basicPublish(
              exchange,
              routingKey,
              elem.mandatory,
              elem.immediate,
              props,
              elem.bytes.toArray
            )

            val expectedResponses: Int = {
              val headers = props.getHeaders
              if (headers == null) {
                responsesPerMessage
              } else {
                val r = headers.get("expectedReplies")
                if (r != null) {
                  r.asInstanceOf[Int]
                } else {
                  responsesPerMessage
                }
              }
            }

            outstandingMessages += expectedResponses
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

  override def toString: String = "AmqpRpcFlow"

}
