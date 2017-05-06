/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.azure.storagequeue

import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, OutHandler}
import akka.stream.{Attributes, FlowShape, Inlet, Outlet}
import scala.concurrent.{ExecutionContext, Future}

/** This flow does just a map
  * with the exception that is also hands the ExecutionContext
  * of the materializer to the function f
  */
private[storagequeue] class FlowMapECStage[In, Out](f: (In, ExecutionContext) => Out)
    extends GraphStage[FlowShape[In, Out]] {
  private val in = Inlet[In]("in")
  private val out = Outlet[Out]("out")
  override val shape = FlowShape(in, out)

  override def createLogic(inheritedAttributes: Attributes) = new GraphStageLogic(shape) {
    setHandler(out, new OutHandler {
      override def onPull() =
        tryPull(in)
    })

    setHandler(in, new InHandler {
      def onPush() = {
        implicit val executionContext = materializer.executionContext
        val input = grab(in)
        push(out, f(input, executionContext))
      }
    })
  }
}
