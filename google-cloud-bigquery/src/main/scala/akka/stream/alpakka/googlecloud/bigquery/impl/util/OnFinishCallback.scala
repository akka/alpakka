/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.bigquery.impl.util
import akka.annotation.InternalApi
import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, OutHandler}
import akka.stream.{Attributes, FlowShape, Inlet, Outlet}

@InternalApi
private[impl] case class OnFinishCallback[T](callBack: T => Unit) extends GraphStage[FlowShape[T, T]] {

  val in = Inlet[T]("OnFinishCallback.in")
  val out = Outlet[T]("OnFinishCallback.out")

  override val shape = FlowShape.of(in, out)

  override def createLogic(attr: Attributes): GraphStageLogic = new GraphStageLogic(shape) {
    private var lastData: Option[T] = None

    setHandler(
      in,
      new InHandler {
        override def onPush(): Unit = {
          val element = grab(in)
          lastData = Some(element)
          push(out, element)
        }

        override def onUpstreamFinish(): Unit = {
          lastData.foreach(callBack)
          super.onUpstreamFinish()
        }
      }
    )

    setHandler(out, new OutHandler {
      override def onPull(): Unit =
        pull(in)
    })
  }
}
