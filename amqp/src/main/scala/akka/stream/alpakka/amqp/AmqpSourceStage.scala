/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.amqp

import java.util.concurrent.atomic.AtomicInteger

import akka.stream._
import akka.stream.stage._
import akka.util.ByteString
import com.rabbitmq.client.AMQP.BasicProperties
import com.rabbitmq.client._

import scala.collection.mutable

final case class IncomingMessage(bytes: ByteString, envelope: Envelope, properties: BasicProperties)
final case class CommittableIncomingMessage(message: IncomingMessage,
                                            callback: AsyncCallback[Unit] = (_: Unit) => Unit)(implicit channel: Channel) {
  def ack(multiple: Boolean = false) = {
    channel.basicAck(message.envelope.getDeliveryTag, multiple)
    callback.invoke(Unit)
  }
  def nack(multiple: Boolean = false, requeue: Boolean = true) = {
    channel.basicNack(message.envelope.getDeliveryTag, multiple, requeue)
    callback.invoke(Unit)
  }
}

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
    extends GraphStage[SourceShape[CommittableIncomingMessage]]
    with AmqpConnector { stage =>

  val out = Outlet[CommittableIncomingMessage]("AmqpSource.out")

  override val shape: SourceShape[CommittableIncomingMessage] = SourceShape.of(out)

  override protected def initialAttributes: Attributes = AmqpSourceStage.defaultAttributes

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) with AmqpConnectorLogic {

      override val settings = stage.settings
      override def connectionFactoryFrom(settings: AmqpConnectionSettings) = stage.connectionFactoryFrom(settings)
      override def newConnection(factory: ConnectionFactory, settings: AmqpConnectionSettings) =
        stage.newConnection(factory, settings)

      private val queue = mutable.Queue[CommittableIncomingMessage]()
      private val unackedMessages = new AtomicInteger()

      override def whenConnected(): Unit = {
        import scala.collection.JavaConverters._
        // we have only one consumer per connection so global is ok
        channel.basicQos(bufferSize, true)
        val consumerCallback = getAsyncCallback(handleDelivery)
        val shutdownCallback = getAsyncCallback[Option[ShutdownSignalException]] {
          case Some(ex) => failStage(ex)
          case None => if (unackedMessages.get() == 0) completeStage()
        }

        val commitCallback = getAsyncCallback[Unit] { (_) =>
          unackedMessages.decrementAndGet()
          if (unackedMessages.get() == 0) completeStage()
        }

        val amqpSourceConsumer = new DefaultConsumer(channel) {
          override def handleDelivery(consumerTag: String,
                                      envelope: Envelope,
                                      properties: BasicProperties,
                                      body: Array[Byte]): Unit =
            consumerCallback.invoke(
              CommittableIncomingMessage(
                IncomingMessage(ByteString(body), envelope, properties),
                commitCallback
              )
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
        } else {
          if (queue.size + 1 > bufferSize) {
            failStage(new RuntimeException(s"Reached maximum buffer size $bufferSize"))
          } else {
            queue.enqueue(message)
          }
        }

      setHandler(out, new OutHandler {
        override def onPull(): Unit =
          if (queue.nonEmpty) {
            pushMessage(queue.dequeue())
          }

      })

      def pushMessage(message: CommittableIncomingMessage): Unit = push(out, message)

      override def onFailure(ex: Throwable): Unit = {}
    }

}
