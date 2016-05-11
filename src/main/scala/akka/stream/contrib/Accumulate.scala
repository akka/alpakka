/*
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.contrib

import akka.japi.function
import akka.stream.{ Attributes, FlowShape, Inlet, Outlet }
import akka.stream.stage.{ GraphStage, GraphStageLogic, InHandler, OutHandler }

/**
 * This companion defines a factory for [[Accumulate]] instances, see [[Accumulate.apply]].
 */
object Accumulate {

  /**
   * Factory for [[Accumulate]] instances.
   *
   * @param zero zero value for folding
   * @param f binary operation for folding
   * @tparam A input type
   * @tparam B output type
   * @return [[Accumulate]] instance
   */
  def apply[A, B](zero: B)(f: (B, A) => B): Accumulate[A, B] = new Accumulate(zero)(f)

  /**
   * Java API: Factory for [[Accumulate]] instances.
   *
   * @param zero zero value for folding
   * @param f binary operation for folding
   * @tparam A input type
   * @tparam B output type
   * @return [[Accumulate]] instance
   */
  def create[A, B](zero: B, f: function.Function2[B, A, B]): Accumulate[A, B] = new Accumulate(zero)(f.apply)
}

/**
 * This stage emits folded values like `scan`, but the first element emitted is not the zero value but the result of
 * applying the given function to the given zero value and the first pushed element.
 *
 * @param zero zero value for folding
 * @param f binary operation for folding
 * @tparam A input type
 * @tparam B output type
 */
final class Accumulate[A, B] private (zero: B)(f: (B, A) => B) extends GraphStage[FlowShape[A, B]] {

  override val shape = FlowShape(Inlet[A]("accumulate.in"), Outlet[B]("accumulate.out"))

  override def createLogic(attributes: Attributes) = new GraphStageLogic(shape) {
    import shape._

    private var acc = zero

    setHandler(in, new InHandler {
      override def onPush() = {
        acc = f(acc, grab(in))
        push(out, acc)
      }
    })

    setHandler(out, new OutHandler {
      override def onPull() = pull(in)
    })
  }
}
