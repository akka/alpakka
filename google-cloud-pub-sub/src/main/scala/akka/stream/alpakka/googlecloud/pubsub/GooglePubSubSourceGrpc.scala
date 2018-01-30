/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.pubsub

import java.util.concurrent.atomic.AtomicInteger

import akka.actor.ActorSystem
import akka.stream.{Attributes, Materializer, Outlet, SourceShape}
import akka.stream.stage.{GraphStage, GraphStageLogic, OutHandler}

import scala.util.{Failure, Success, Try}
import GooglePubSubSourceGrpc._
import com.google.pubsub.v1

import scala.collection.JavaConverters._
import scala.collection.immutable

@akka.annotation.InternalApi
private final class GooglePubSubSourceGrpc(parallelism: Int, grpcApi: GrpcApi)(
    implicit as: ActorSystem
) extends GraphStage[SourceShape[v1.ReceivedMessage]] {

  val out: Outlet[v1.ReceivedMessage] = Outlet("GooglePubSubSourceGrpc.out")
  override val shape: SourceShape[v1.ReceivedMessage] = SourceShape(out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) {
      private var state: State = Pending
      private val fetches: AtomicInteger = new AtomicInteger(0)

      def fetch(implicit mat: Materializer): Unit = {
        import mat.executionContext
        fetches.incrementAndGet()

        grpcApi.read.onComplete { tr =>
          fetches.decrementAndGet()
          callback.invoke(tr -> mat)
        }

        state = Fetching
      }

      private val callback = getAsyncCallback[(Try[v1.PullResponse], Materializer)] {
        case (Failure(tr), _) =>
          failStage(tr)
        case (Success(response), mat) =>
          response.getReceivedMessagesList.asScala.toList match {
            case head :: tail =>
              state match {
                case HoldingMessages(oldHead :: oldTail) =>
                  state = HoldingMessages(oldTail ++ Seq(head) ++ tail)
                  push(out, oldHead)
                case _ =>
                  state = HoldingMessages(tail)
                  push(out, head)
              }
            case Nil =>
              state = Fetching
              fetch(mat)
          }
      }

      setHandler(
        out,
        new OutHandler {
          override def onPull(): Unit =
            state match {
              case Pending =>
                state = Fetching
                fetch(materializer)

              case Fetching if fetches.get() < parallelism =>
                state = Fetching
                fetch(materializer)

              case Fetching if fetches.get() > parallelism =>
              // do nothing we will push on request result

              case HoldingMessages(xs) =>
                xs match {
                  case head :: tail =>
                    state = HoldingMessages(tail)
                    push(out, head)
                  case Nil =>
                    state = Fetching
                    fetch(materializer)
                }
            }
        }
      )
    }
}

@akka.annotation.InternalApi
private object GooglePubSubSourceGrpc {
  private sealed trait State
  private case object Pending extends State
  private case object Fetching extends State
  private case class HoldingMessages(xs: immutable.Seq[v1.ReceivedMessage]) extends State
}
