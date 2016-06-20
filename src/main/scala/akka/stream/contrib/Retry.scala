/*
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.contrib

import akka.stream._
import akka.stream.scaladsl._
import akka.stream.stage._
import scala.util.{ Try, Success, Failure }
import scala.concurrent.Future
import scala.collection.immutable

/**
 * This object defines a factory methods for retry operaions.
 */
object Retry {

  /**
   * Retry flow factory. given a flow that produces `Try`s, this wrapping flow may be used to try
   * and pass failed elements through the flow again. More accurately, the given flow consumes a tuple
   * of `input` & `state`, and produces a tuple of `Try` of `output` and `state`.
   * If the flow emits a failed element (i.e. `Try` is a `Failure`), the `retryWith` function is fed with the
   * `state` of the failed element, and may produce a new input-state tuple to pass through the original flow.
   * The function may also yield `None` instead of `Some((input,state))`, which means not to retry a failed element.
   *
   * IMPORTANT CAVEAT:
   * The given flow must not change the number of elements passing through it (i.e. it should output
   * exactly one element for every received element). Ignoring this, will have an unpredicted result,
   * and may result in a deadlock.
   *
   * @param flow the flow to retry
   * @param retryWith if output was failure, we can optionaly recover from it,
   *        and retry with a new pair of input & new state we get from this function.
   * @tparam I input elements type
   * @tparam O output elements type
   * @tparam S state to create a new `(I,S)` to retry with
   * @tparam M materialized value type
   */
  def apply[I, O, S, M](flow: Graph[FlowShape[(I, S), (Try[O], S)], M])(retryWith: S => Option[(I, S)]): Graph[FlowShape[(I, S), (Try[O], S)], M] = {
    GraphDSL.create(flow) { implicit b => origFlow =>
      import GraphDSL.Implicits._

      val retry = b.add(new RetryCoordinator[I, S, O](retryWith))

      retry.out2 ~> origFlow ~> retry.in2

      FlowShape(retry.in1, retry.out1)
    }
  }

  /**
   * Factory for multiple retries flow. similar to the simple retry, but this will allow to
   * break down a "heavy" element which failed into multiple "thin" elements, that may succeed individually.
   * Since it's easy to inflate elements in retry cycle, there's also a limit parameter,
   * that will limit the amount of generated elements by the `retryWith` function,
   * and will fail the stage if that limit is exceeded.
   * Passing `Some(Nil)` is valid, and will result in filtering out the failure quietly, without
   * emitting a failed `Try` element.
   *
   * IMPORTANT CAVEAT:
   * The given flow must not change the number of elements passing through it (i.e. it should output
   * exactly one element for every received element). Ignoring this, will have an unpredicted result,
   * and may result in a deadlock.
   *
   * @param limit since every retry can generate more elements,
   *        the inner queue can get too big. if the limit is reached,
   *        the stage will fail.
   * @param flow the flow to retry
   * @param retryWith if output was failure, we can optionaly recover from it,
   *        and retry with a sequence of input & new state pairs we get from this function.
   * @tparam I input elements type
   * @tparam O output elements type
   * @tparam S state to create a new `(I,S)` to retry with
   * @tparam M materialized value type
   */
  def concat[I, O, S, M](limit: Long, flow: Graph[FlowShape[(I, S), (Try[O], S)], M])(retryWith: S => Option[immutable.Iterable[(I, S)]]): Graph[FlowShape[(I, S), (Try[O], S)], M] = {
    GraphDSL.create(flow) { implicit b => origFlow =>
      import GraphDSL.Implicits._

      val retry = b.add(new RetryConcatCoordinator[I, S, O](limit, retryWith))

      retry.out2 ~> origFlow ~> retry.in2

      FlowShape(retry.in1, retry.out1)
    }
  }

  private[akka] class RetryCoordinator[I, S, O](retryWith: S => Option[(I, S)]) extends GraphStage[BidiShape[(I, S), (Try[O], S), (Try[O], S), (I, S)]] {
    val in1 = Inlet[(I, S)]("Retry.ext.in")
    val out1 = Outlet[(Try[O], S)]("Retry.ext.out")
    val in2 = Inlet[(Try[O], S)]("Retry.int.in")
    val out2 = Outlet[(I, S)]("Retry.int.out")
    override val shape = BidiShape[(I, S), (Try[O], S), (Try[O], S), (I, S)](in1, out1, in2, out2)
    override def createLogic(attributes: Attributes) = new GraphStageLogic(shape) {
      var elementInCycle = false
      var pending: (I, S) = null

      setHandler(in1, new InHandler {
        override def onPush() = {
          val is = grab(in1)
          if (!hasBeenPulled(in2)) pull(in2)
          push(out2, is)
          elementInCycle = true
        }

        override def onUpstreamFinish() = {
          if (!elementInCycle)
            completeStage()
        }
      })

      setHandler(out1, new OutHandler {
        override def onPull() = {
          if (isAvailable(out2)) pull(in1)
          else pull(in2)
        }
      })

      setHandler(in2, new InHandler {
        override def onPush() = {
          elementInCycle = false
          grab(in2) match {
            case s @ (_: Success[O], _) => pushAndCompleteIfLast(s)
            case failure @ (_, s) => retryWith(s).fold(pushAndCompleteIfLast(failure)) { is =>
              pull(in2)
              if (isAvailable(out2)) {
                push(out2, is)
                elementInCycle = true
              } else pending = is
            }
          }
        }
      })

      def pushAndCompleteIfLast(elem: (Try[O], S)): Unit = {
        push(out1, elem)
        if (isClosed(in1))
          completeStage()
      }

      setHandler(out2, new OutHandler {
        override def onPull() = {
          if (isAvailable(out1) && !elementInCycle) {
            if (pending ne null) {
              push(out2, pending)
              pending = null
              elementInCycle = true
            } else if (!hasBeenPulled(in1)) {
              pull(in1)
            }
          }
        }

        override def onDownstreamFinish() = {
          //Do Nothing, intercept completion as downstream
        }
      })
    }
  }

  private[akka] class RetryConcatCoordinator[I, S, O](limit: Long, retryWith: S => Option[immutable.Iterable[(I, S)]]) extends GraphStage[BidiShape[(I, S), (Try[O], S), (Try[O], S), (I, S)]] {
    val in1 = Inlet[(I, S)]("RetryConcat.ext.in")
    val out1 = Outlet[(Try[O], S)]("RetryConcat.ext.out")
    val in2 = Inlet[(Try[O], S)]("RetryConcat.int.in")
    val out2 = Outlet[(I, S)]("RetryConcat.int.out")
    override val shape = BidiShape[(I, S), (Try[O], S), (Try[O], S), (I, S)](in1, out1, in2, out2)
    override def createLogic(attributes: Attributes) = new GraphStageLogic(shape) {
      var elementInCycle = false
      val queue = scala.collection.mutable.Queue.empty[(I, S)]

      setHandler(in1, new InHandler {
        override def onPush() = {
          val is = grab(in1)
          if (!hasBeenPulled(in2)) pull(in2)
          if (isAvailable(out2)) {
            push(out2, is)
            elementInCycle = true
          } else queue.enqueue(is)
        }

        override def onUpstreamFinish() = {
          if (!elementInCycle && queue.isEmpty)
            completeStage()
        }
      })

      setHandler(out1, new OutHandler {
        override def onPull() = {
          if (queue.isEmpty) {
            if (isAvailable(out2)) pull(in1)
            else pull(in2)
          } else {
            pull(in2)
            if (isAvailable(out2)) {
              push(out2, queue.dequeue())
              elementInCycle = true
            }
          }
        }
      })

      setHandler(in2, new InHandler {
        override def onPush() = {
          elementInCycle = false
          grab(in2) match {
            case s @ (_: Success[O], _) => pushAndCompleteIfLast(s)
            case failure @ (_, s) => retryWith(s).fold(pushAndCompleteIfLast(failure)) { xs =>
              if (xs.size + queue.size > limit) failStage(new IllegalStateException(s"Queue limit of $limit has been exceeded. Trying to append ${xs.size} elements to a queue that has ${queue.size} elements."))
              else {
                xs.foreach(queue.enqueue(_))
                if (queue.isEmpty) {
                  if (isClosed(in1)) completeStage()
                  else pull(in1)
                } else {
                  pull(in2)
                  if (isAvailable(out2)) {
                    push(out2, queue.dequeue())
                    elementInCycle = true
                  }
                }
              }
            }
          }
        }
      })

      def pushAndCompleteIfLast(elem: (Try[O], S)): Unit = {
        push(out1, elem)
        if (isClosed(in1) && queue.isEmpty)
          completeStage()
      }

      setHandler(out2, new OutHandler {
        override def onPull() = {
          if (!elementInCycle && isAvailable(out1)) {
            if (queue.isEmpty) pull(in1)
            else {
              push(out2, queue.dequeue())
              elementInCycle = true
              if (!hasBeenPulled(in2)) pull(in2)
            }
          }
        }

        override def onDownstreamFinish() = {
          //Do Nothing, intercept completion as downstream
        }
      })
    }
  }
}
