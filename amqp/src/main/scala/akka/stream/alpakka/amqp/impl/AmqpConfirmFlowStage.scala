/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.amqp.impl

import akka.Done
import akka.stream._
import akka.stream.alpakka.amqp.{AmqpWriteSettings, ConfirmMessage, WriteMessage}
import akka.stream.stage.{GraphStageLogic, GraphStageWithMaterializedValue, InHandler, OutHandler}
import com.rabbitmq.client.ConfirmListener

import scala.collection.mutable
import scala.concurrent.{Future, Promise}

class AmqpConfirmFlowStage(settings: AmqpWriteSettings)
    extends GraphStageWithMaterializedValue[FlowShape[WriteMessage, ConfirmMessage], Future[Done]] {

  private val in = Inlet[WriteMessage]("AmqpConfirmFlow.in")
  private val out = Outlet[ConfirmMessage]("AmqpConfirmFlow.out")

  val shape: FlowShape[WriteMessage, ConfirmMessage] = FlowShape.of(in, out)

  def createLogicAndMaterializedValue(attributes: Attributes): (GraphStageLogic, Future[Done]) = {
    val promise = Promise[Done]()

    val exchange = settings.exchange.getOrElse("")
    val routingKey = settings.routingKey.getOrElse("")

    (new AmqpConnectorLogic(shape, settings) {

      private val buffer = mutable.Queue[ConfirmMessage]()

      private val callback = getAsyncCallback[ConfirmMessage] { elem =>
        if (isAvailable(out)) {
          push(out, elem)
          if (!hasBeenPulled(in)) {
            pull(in)
          }
        } else {
          buffer.enqueue(elem)
        }
      }

      def whenConnected(): Unit = {
        channel.confirmSelect()
        channel.addConfirmListener(new ConfirmListener {
          def handleAck(deliveryTag: Long, multiple: Boolean): Unit =
            callback.invoke(ConfirmMessage.Ack(deliveryTag, multiple))

          def handleNack(deliveryTag: Long, multiple: Boolean): Unit =
            callback.invoke(ConfirmMessage.Nack(deliveryTag, multiple))
        })
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
          def onPush(): Unit = {
            val elem = grab(in)
            channel.basicPublish(
              exchange,
              elem.routingKey.getOrElse(routingKey),
              elem.mandatory,
              elem.immediate,
              elem.properties.orNull,
              elem.bytes.toArray
            )
          }

          override def onUpstreamFailure(ex: Throwable): Unit = {
            promise.tryFailure(ex)
            super.onUpstreamFailure(ex)
          }

          override def onUpstreamFinish(): Unit = {
            promise.trySuccess(Done)
            super.onUpstreamFinish()
          }
        }
      )

      setHandler(
        out,
        new OutHandler {
          def onPull(): Unit = {
            while (buffer.nonEmpty && isAvailable(out)) {
              push(out, buffer.dequeue())
            }
            if (buffer.isEmpty && !hasBeenPulled(in)) {
              pull(in)
            }
          }
        }
      )

    }, promise.future)
  }

}
