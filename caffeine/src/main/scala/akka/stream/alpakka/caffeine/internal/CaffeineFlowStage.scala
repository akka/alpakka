/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.caffeine.internal

import akka.stream.alpakka.caffeine.Aggregator

import akka.stream.{Attributes, FlowShape, Inlet, Outlet}
import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, OutHandler}

import scala.language.postfixOps

class CaffeineFlowStage[K, V, R](aggregator: Aggregator[K, V, R]) extends GraphStage[FlowShape[V, Either[R, R]]] {

  private val in = Inlet[V]("in")
  private val out = Outlet[Either[R, R]]("out")

  override val shape = new FlowShape(in, out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new CaffeineGraphLogic[K, V, R](shape, aggregator, in) with InHandler {

      override final def pumpComplete(): Boolean =
        isAvailable(out) && {
          if (completeQueue.isEmpty) {
            if (!isClosed(in) && !hasBeenPulled(in))
              pull(in)
            else if (expiredQueue.isEmpty) checkComplete(out)
            false
          } else {
            val e = completeQueue.dequeue()
            push(out, Right(e))
            true
          }
        }

      override final def pumpExp(): Boolean =
        isAvailable(out) && {
          if (expiredQueue.isEmpty) {
            if (!isClosed(in) && !hasBeenPulled(in))
              pull(in)
            else if (completeQueue.isEmpty) checkComplete(out)
            false
          } else {
            val e = expiredQueue.dequeue()
            push(out, Left(e))
            true
          }
        }

      setHandler(out, new OutHandler {
        override def onPull(): Unit =
          pump()
      })

    }
}
