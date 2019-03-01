/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.unixdomainsocket.impl

import akka.actor.ActorRef
import akka.stream._
import akka.stream.scaladsl._
import akka.stream.stage._

import scala.util._
import scala.util.control.NonFatal

class WrappedFlowStageWithMaterializedValue[In, Out, Mat](
    f: () => (Flow[In, Out, Any], Mat),
    in: Inlet[In],
    out: Outlet[Out]
) extends GraphStageWithMaterializedValue[FlowShape[In, Out], Mat] {

  override val shape = FlowShape(in, out)

  override def createLogicAndMaterializedValue(inheritedAttributes: Attributes): (GraphStageLogic, Mat) = {
    val (flow, mat) = f()
    val logic = new GraphStageLogic(shape) with InHandler with OutHandler {
      implicit def ec = materializer.executionContext

      var sink: SourceQueueWithComplete[In] = _
      var source: SinkQueueWithCancel[Out] = _

      setHandler(in, this)
      setHandler(out, this)

      override def preStart(): Unit = {
        Source
          .queue(1, OverflowStrategy.backpressure)
          .via(flow)
          .toMat(Sink.queue())(Keep.both)
          .run()(materializer) match {
          case (si, so) =>
            sink = si
            source = so
        }

        getStageActor(behavior(None))
      }

      override def onPush(): Unit =
        sink
          .offer(grab(in))
          .recover { case NonFatal(e) => QueueOfferResult.Failure(e) }
          .foreach(stageActor.ref ! _)

      override def onPull(): Unit =
        pull(in)

      override def onUpstreamFinish(): Unit =
        stageActor.become(behavior(Some(Success {})))

      override def onUpstreamFailure(ex: Throwable): Unit =
        stageActor.become(behavior(Some(Failure(ex))))

      override def onDownstreamFinish(): Unit = {
        source.cancel()
        completeStage()
      }

      private def behavior(complete: Option[Try[Unit]]): ((ActorRef, Any)) => Unit = {
        case (_, QueueOfferResult.Enqueued) =>
          source.pull().foreach(stageActor.ref ! _)
          stageActor.become(behavior(complete))

        case (_, QueueOfferResult.Dropped) =>
          failStage(new IllegalStateException("Should not be dropped"))

        case (_, QueueOfferResult.QueueClosed) =>
          cancel(in)

        case (_, QueueOfferResult.Failure(e)) =>
          failStage(e)

        case (_, Some(e)) =>
          push(out, e.asInstanceOf[Out])
          complete.foreach {
            case Success(_) =>
              sink.complete()
              completeStage()
            case Failure(ex) =>
              sink.fail(ex)
              failStage(ex)
          }

        case (_, None) =>
          val ex = new IllegalStateException("Pulled nothing")
          sink.fail(ex)
          failStage(ex)
      }

    }

    (logic, mat)
  }

}
