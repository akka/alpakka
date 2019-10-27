/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.amqp.impl

import akka.Done
import akka.annotation.InternalApi
import akka.event.Logging
import akka.stream.{ActorAttributes, Attributes, FlowShape, Inlet, Outlet}
import akka.stream.alpakka.amqp.{AmqpWriteSettings, WriteMessage, WriteResult}
import akka.stream.stage.{GraphStageLogic, GraphStageWithMaterializedValue}

import scala.concurrent.{Future, Promise}

/**
 * Internal API.
 *
 * Simplest AMQP flow. Published messages without any delivery guarantees. Result is emitted as soon as message is
 * published. Because there it's not verified whether message is confirmed or rejected, results are emitted always
 * as confirmed. Since result is always confirmed, it would also make sense to emit just plain `passThrough` object
 * instead of complete [[WriteResult]] (possibly it would be less confusing for users), but [[WriteResult]] is used
 * for consistency with other variants and to make the flow ready for any possible future [[WriteResult]] extensions.
 */
@InternalApi private[amqp] final class AmqpSimpleFlowStage[T](settings: AmqpWriteSettings)
    extends GraphStageWithMaterializedValue[FlowShape[(WriteMessage, T), (WriteResult, T)], Future[Done]] { stage =>

  private val in: Inlet[(WriteMessage, T)] = Inlet(Logging.simpleName(this) + ".in")
  private val out: Outlet[(WriteResult, T)] = Outlet(Logging.simpleName(this) + ".out")

  override val shape: FlowShape[(WriteMessage, T), (WriteResult, T)] = FlowShape.of(in, out)

  override protected def initialAttributes: Attributes =
    Attributes.name(Logging.simpleName(this)) and ActorAttributes.IODispatcher

  override def createLogicAndMaterializedValue(inheritedAttributes: Attributes): (GraphStageLogic, Future[Done]) = {
    val streamCompletion = Promise[Done]()
    (new AbstractAmqpFlowStageLogic[T](settings, streamCompletion, shape) {
      override def publish(message: WriteMessage, passThrough: T): Unit = {
        log.debug("Publishing message {}.", message)

        channel.basicPublish(
          settings.exchange.getOrElse(""),
          message.routingKey.orElse(settings.routingKey).getOrElse(""),
          message.mandatory,
          message.immediate,
          message.properties.orNull,
          message.bytes.toArray
        )
        push(out, (WriteResult.confirmed, passThrough))
      }
    }, streamCompletion.future)
  }
}
