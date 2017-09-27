/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.amqp

import akka.actor.ActorRef
import akka.stream._
import akka.stream.stage._
import akka.util.{ByteString, Timeout}
import com.rabbitmq.client.AMQP.BasicProperties
import com.rabbitmq.client._

import scala.collection.mutable
import scala.concurrent.duration._

final case class IncomingMessage[A](bytes: A, envelope: Envelope, properties: BasicProperties)

object UnackedIncomingMessage {
  final case class Ack(deliveryTag: Long) extends Command(c => c.basicAck(deliveryTag, false))
  final case class Nack(deliveryTag: Long, requeue: Boolean)
      extends Command(c => c.basicNack(deliveryTag, false, requeue))

  class Command[A](f: (Channel) => A) extends ((Channel) => A) {
    override def apply(c: Channel): A = f(c)
  }

}

final case class UnackedIncomingMessage[A](message: IncomingMessage[A], ref: ActorRef) {
  import UnackedIncomingMessage._

  /**
   * Ack this message with the channel that delivered it
   */
  def ack(): IncomingMessage[A] = {
    ref ! Ack(message.envelope.getDeliveryTag)
    message
  }

  def nack(requeue: Boolean): IncomingMessage[A] = {
    ref ! Nack(message.envelope.getDeliveryTag, requeue)
    message
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
    extends GraphStage[SourceShape[IncomingMessage[ByteString]]]
    with AmqpConnector { stage =>

  val out = Outlet[IncomingMessage[ByteString]]("AmqpSource.out")

  override val shape: SourceShape[IncomingMessage[ByteString]] = SourceShape.of(out)

  override protected def initialAttributes: Attributes = AmqpSourceStage.defaultAttributes

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) with AmqpConnectorLogic {

      override val settings = stage.settings
      override def connectionFactoryFrom(settings: AmqpConnectionSettings) = stage.connectionFactoryFrom(settings)
      override def newConnection(factory: ConnectionFactory, settings: AmqpConnectionSettings) =
        stage.newConnection(factory, settings)

      override def closeConnection(
          settings: AmqpConnectionSettings,
          connection: Connection
      ): Unit = stage.closeConnection(settings, connection)

      private val queue = mutable.Queue[IncomingMessage[ByteString]]()

      override def whenConnected(): Unit = {
        import scala.collection.JavaConverters._
        // we have only one consumer per connection so global is ok
        channel.basicQos(bufferSize, true)
        val consumerCallback = getAsyncCallback(handleDelivery)
        val shutdownCallback = getAsyncCallback[Option[ShutdownSignalException]] {
          case Some(ex) => failStage(ex)
          case None => completeStage()
        }

        val amqpSourceConsumer = new DefaultConsumer(channel) {
          override def handleDelivery(consumerTag: String,
                                      envelope: Envelope,
                                      properties: BasicProperties,
                                      body: Array[Byte]): Unit =
            consumerCallback.invoke(IncomingMessage(ByteString(body), envelope, properties))

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

      def handleDelivery(message: IncomingMessage[ByteString]): Unit =
        if (isAvailable(out)) {
          pushAndAckMessage(message)
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
            pushAndAckMessage(queue.dequeue())
          }

      })

      def pushAndAckMessage(message: IncomingMessage[ByteString]): Unit = {
        push(out, message)
        // ack it as soon as we have passed it downstream
        // TODO ack less often and do batch acks with multiple = true would probably be more performant
        channel.basicAck(
          message.envelope.getDeliveryTag,
          false // just this single message
        )
      }
      override def onFailure(ex: Throwable): Unit = {}
    }

}

object AmqpSourceWithoutAckStage {

  private val defaultAttributes = Attributes.name("AmqpSourceWithoutAck")

}

/**
 * Connects to an AMQP server upon materialization and consumes messages from it emitting them
 * into the stream. Each materialized source will create one connection to the broker.
 * As soon as an `IncomingMessage` is sent downstream, an ack for it is sent to the broker.
 *
 * @param bufferSize The max number of elements to prefetch and buffer at any given time.
 */
final class AmqpSourceWithoutAckStage(settings: AmqpSourceSettings, bufferSize: Int)
    extends GraphStage[SourceShape[UnackedIncomingMessage[ByteString]]]
    with AmqpConnector { stage =>

  val out = Outlet[UnackedIncomingMessage[ByteString]]("AmqpSourceWithoutAck.out")

  override val shape: SourceShape[UnackedIncomingMessage[ByteString]] = SourceShape.of(out)

  override protected def initialAttributes: Attributes = AmqpSourceWithoutAckStage.defaultAttributes

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) with AmqpConnectorLogic {

      override val settings = stage.settings
      override def connectionFactoryFrom(settings: AmqpConnectionSettings) = stage.connectionFactoryFrom(settings)
      override def newConnection(factory: ConnectionFactory, settings: AmqpConnectionSettings) =
        stage.newConnection(factory, settings)

      override def closeConnection(
          settings: AmqpConnectionSettings,
          connection: Connection
      ): Unit = stage.closeConnection(settings, connection)

      private val queue = mutable.Queue[UnackedIncomingMessage[ByteString]]()

      override def whenConnected(): Unit = {

        import UnackedIncomingMessage._
        val ref = getStageActor {
          case (actor, msg: Command[_]) =>
            msg(channel)

        }.ref
        import scala.collection.JavaConverters._
        // we have only one consumer per connection so global is ok
        channel.basicQos(bufferSize, true)

        val consumerCallback = getAsyncCallback(handleDelivery)
        val shutdownCallback = getAsyncCallback[Option[ShutdownSignalException]] {
          case Some(ex) => failStage(ex)
          case None => completeStage()
        }

        val amqpSourceConsumer = new DefaultConsumer(channel) {
          override def handleDelivery(consumerTag: String,
                                      envelope: Envelope,
                                      properties: BasicProperties,
                                      body: Array[Byte]): Unit =
            consumerCallback.invoke(
              UnackedIncomingMessage(IncomingMessage(ByteString(body), envelope, properties), ref)
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

      def handleDelivery(message: UnackedIncomingMessage[ByteString]): Unit =
        if (isAvailable(out)) {
          push(out, message)
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
            push(out, queue.dequeue())
          }

      })

    }

}
