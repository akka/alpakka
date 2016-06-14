/*
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.contrib

import akka.japi.function
import akka.stream.{ Attributes, FanOutShape2 }
import akka.stream.stage.{ GraphStage, GraphStageLogic, InHandler, OutHandler }

/**
 * This companion defines a factory for [[PartitionWith]] instances, see [[PartitionWith.apply]].
 */
object PartitionWith {

  /**
   * Factory for [[PartitionWith]] instances.
   *
   * @param p partition function
   * @tparam In input type
   * @tparam Out0 left output type
   * @tparam Out1 right output type
   * @return [[PartitionWith]] instance
   */
  def apply[In, Out0, Out1](p: In => Either[Out0, Out1]): PartitionWith[In, Out0, Out1] = new PartitionWith(p)

  /**
   * Java API: Factory for [[PartitionWith]] instances.
   *
   * @param p partition function
   * @tparam In input type
   * @tparam Out0 left output type
   * @tparam Out1 right output type
   * @return [[PartitionWith]] instance
   */
  def create[In, Out0, Out1](p: function.Function[In, Either[Out0, Out1]]): PartitionWith[In, Out0, Out1] = new PartitionWith(p.apply)
}

/**
 * This stage partitions input to 2 different outlets,
 * applying different transformations on the elements,
 * according to the received partition function.
 *
 * @param p partition function
 * @tparam In input type
 * @tparam Out0 left output type
 * @tparam Out1 right output type
 */
final class PartitionWith[In, Out0, Out1] private (p: In => Either[Out0, Out1]) extends GraphStage[FanOutShape2[In, Out0, Out1]] {

  override val shape = new FanOutShape2[In, Out0, Out1]("partitionWith")

  override def createLogic(attributes: Attributes) = new GraphStageLogic(shape) {
    import shape._

    private var pending: Either[Out0, Out1] = null

    setHandler(in, new InHandler {
      override def onPush() = {
        val elem = grab(in)
        p(elem) match {
          case Left(o) if isAvailable(out0) =>
            push(out0, o)
            if (isAvailable(out1))
              pull(in)
          case Right(o) if isAvailable(out1) =>
            push(out1, o)
            if (isAvailable(out0))
              pull(in)
          case either =>
            pending = either
        }
      }

      override def onUpstreamFinish() = {
        if (pending eq null)
          completeStage()
      }
    })

    setHandler(out0, new OutHandler {
      override def onPull() = if (pending ne null) pending.left.foreach { o =>
        push(out0, o)
        if (isClosed(in)) completeStage()
        else {
          pending = null
          if (isAvailable(out1))
            pull(in)
        }
      }
      else if (!hasBeenPulled(in)) pull(in)
    })

    setHandler(out1, new OutHandler {
      override def onPull() = if (pending ne null) pending.right.foreach { o =>
        push(out1, o)
        if (isClosed(in)) completeStage()
        else {
          pending = null
          if (isAvailable(out0))
            pull(in)
        }
      }
      else if (!hasBeenPulled(in)) pull(in)
    })
  }
}
