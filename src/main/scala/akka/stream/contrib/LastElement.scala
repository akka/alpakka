/*
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.contrib

import akka.stream.stage.{ GraphStageLogic, GraphStageWithMaterializedValue, InHandler, OutHandler }
import akka.stream.{ Attributes, FlowShape, Inlet, Outlet }
import scala.concurrent.{ Future, Promise }

/**
 * This companion defines a factory for [[LastElement]] instances, see [[LastElement.apply]].
 */
object LastElement {

  /**
   * Factory for [[LastElement]] instances.
   *
   * @tparam A input and output type
   * @return [[LastElement]] instance
   */
  def apply[A](): LastElement[A] = new LastElement

  /**
   * Java API: Factory for [[LastElement]] instances.
   *
   * @tparam A input and output type
   * @return [[LastElement]] instance
   */
  def create[A](): LastElement[A] = new LastElement
}

/**
 * This stage materializes to the last element pushed before upstream completion, if any, thereby recovering from any
 * failure. Pushed elements are just passed along.
 *
 * @tparam A input and output type
 */
final class LastElement[A] private extends GraphStageWithMaterializedValue[FlowShape[A, A], Future[Option[A]]] {

  override val shape = FlowShape(Inlet[A]("lastElement.in"), Outlet[A]("lastElement.out"))

  override def createLogicAndMaterializedValue(attributes: Attributes) = {

    val matValue = Promise[Option[A]]()

    val logic = new GraphStageLogic(shape) {
      import shape._

      private var currentElement = Option.empty[A]

      setHandler(in, new InHandler {
        override def onPush() = {
          val element = grab(in)
          currentElement = Some(element)
          push(out, element)
        }

        override def onUpstreamFinish() = {
          matValue.success(currentElement)
          super.onUpstreamFinish()
        }

        override def onUpstreamFailure(t: Throwable) = {
          matValue.success(currentElement)
          super.onUpstreamFinish()
        }
      })

      setHandler(out, new OutHandler {
        override def onPull() = pull(in)
      })
    }

    (logic, matValue.future)
  }
}
