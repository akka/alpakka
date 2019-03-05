/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.amqp.impl

import akka.Done
import akka.annotation.InternalApi
import akka.stream.alpakka.amqp.{AmqpPublishConfirmConfiguration, AmqpWriteSettings, WriteMessage}
import akka.stream.stage.{GraphStageLogic, GraphStageWithMaterializedValue, InHandler, OutHandler}
import akka.stream._

import scala.concurrent.{Future, Promise}
import scala.util.{Failure, Success, Try}

/**
 * Connects to an AMQP server upon materialization and sends incoming messages to the server.
 * Each materialized flow will create one connection to the broker.
 */
@InternalApi
private[amqp] final class AmqpPublishFlowStage[O](settings: AmqpWriteSettings)
    extends GraphStageWithMaterializedValue[FlowShape[(WriteMessage, O), O], Future[Done]] { stage =>

  val in = Inlet[(WriteMessage, O)]("AmqpPublishConfirmFlow.in")
  val out = Outlet[O]("AmqpPublishConfirmFlow.out")

  override def shape: FlowShape[(WriteMessage, O), O] = FlowShape.of(in, out)

  override protected def initialAttributes: Attributes =
    Attributes
      .name("AmqpPublishFlowStage")
      .and(ActorAttributes.dispatcher("akka.stream.default-blocking-io-dispatcher"))

  override def createLogicAndMaterializedValue(inheritedAttributes: Attributes): (GraphStageLogic, Future[Done]) = {
    val promise = Promise[Done]()
    (new GraphStageLogic(shape) with AmqpConnectorLogic {
      override val settings = stage.settings
      private val exchange = settings.exchange.getOrElse("")
      private val routingKey = settings.routingKey.getOrElse("")

      override def whenConnected(): Unit =
        if (settings.publishConfirm.isDefined) {
          channel.confirmSelect()
        }

      setHandler(
        out,
        new OutHandler {
          override def onPull(): Unit = pull(in)
        }
      )

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
            val (elem, passthrough) = grab(in)
            channel.basicPublish(
              exchange,
              elem.routingKey.getOrElse(routingKey),
              elem.mandatory,
              elem.immediate,
              elem.properties.orNull,
              elem.bytes.toArray
            )

            settings.publishConfirm match {
              case Some(AmqpPublishConfirmConfiguration(confirmTimeout)) =>
                Try(channel.waitForConfirmsOrDie(confirmTimeout.toMillis)) match {
                  case Success(_) => push(out, passthrough)
                  case Failure(e) => fail(out, e)
                }
              case None => push(out, passthrough)
            }
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

  override def toString: String = "AmqpPublishConfirmFlow"
}
