/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.bigquery.impl.util

import akka.NotUsed
import akka.annotation.InternalApi
import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, OutHandler}
import akka.stream.{Attributes, Graph, Inlet, Outlet, UniformFanOutShape}

@InternalApi
private[impl] object Splitter {
  def apply[T](out0Predicate: T => Boolean): Graph[UniformFanOutShape[T, T], NotUsed] = {
    new Splitter[T](out0Predicate)
  }

  private[impl] class Splitter[T](out0Predicate: T => Boolean) extends GraphStage[UniformFanOutShape[T, T]] {
    val in: Inlet[T] = Inlet[T]("Splitter.in")
    val out0: Outlet[T] = Outlet("Splitter.out0")
    val out1: Outlet[T] = Outlet("Splitter.out1")
    override val shape: UniformFanOutShape[T, T] = UniformFanOutShape(in, out0, out1)

    override def createLogic(attr: Attributes): GraphStageLogic =
      new GraphStageLogic(shape) {
        var pulls = 0
        def pullIfNeeded(): Unit = {
          if (pulls == 2) {
            pull(in)
          }
        }
        setHandler(in,
                   new InHandler {
                     override def onPush(): Unit = {
                       val e = grab(in)
                       if (out0Predicate(e)) {
                         push(out0, e)
                         pulls -= 1;
                       } else {
                         push(out1, e)
                         pulls -= 1;
                       }
                     }
                   }
        )
        setHandler(out0,
                   new OutHandler {
                     override def onPull(): Unit = {
                       pulls += 1
                       pullIfNeeded()
                     }
                   }
        )
        setHandler(out1,
                   new OutHandler {
                     override def onPull(): Unit = {
                       pulls += 1
                       pullIfNeeded()
                     }
                   }
        )
      }
  }
}
