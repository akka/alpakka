/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.pubsub.impl

import akka.NotUsed
import akka.actor.ActorSystem
import akka.annotation.InternalApi
import akka.stream.alpakka.googlecloud.pubsub.impl.GooglePubSubSource._
import akka.stream.alpakka.googlecloud.pubsub.{PullResponse, ReceivedMessage}
import akka.stream.stage.{GraphStage, GraphStageLogic, OutHandler, TimerGraphStageLogic}
import akka.stream.{Attributes, Materializer, Outlet, SourceShape}

import scala.collection.immutable
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}

@InternalApi
private[pubsub] final class GooglePubSubSource(projectId: String,
                                               session: GoogleSession,
                                               subscription: String,
                                               returnImmediately: Boolean,
                                               maxMessages: Int,
                                               httpApi: PubSubApi)(implicit as: ActorSystem)
    extends GraphStage[SourceShape[ReceivedMessage]] {

  val out: Outlet[ReceivedMessage] = Outlet("GooglePubSubSource.out")
  override val shape: SourceShape[ReceivedMessage] = SourceShape(out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new TimerGraphStageLogic(shape) {
      private var state: State = Pending

      def fetch(implicit mat: Materializer): Unit = {
        import mat.executionContext

        def pull(maybeAccessToken: Option[String]): Future[PullResponse] =
          httpApi.pull(projectId, subscription, maybeAccessToken, returnImmediately, maxMessages)

        val req = if (httpApi.isEmulated) {
          pull(None)
        } else {
          session
            .getToken()
            .flatMap { token =>
              pull(Some(token))
            }
        }
        req.onComplete { tr =>
          callback.invoke(tr -> mat)
        }
        state = Fetching
      }

      override def onTimer(timerKey: Any): Unit =
        fetch(materializer)

      private val callback = getAsyncCallback[(Try[PullResponse], Materializer)] {
        case (Failure(tr), _) =>
          failStage(tr)
        case (Success(response), mat) =>
          response.receivedMessages.getOrElse(Nil) match {
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
              scheduleOnce(NotUsed, 1.second)
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
              case Fetching =>
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

@InternalApi
private object GooglePubSubSource {
  private sealed trait State
  private case object Pending extends State
  private case object Fetching extends State
  private case class HoldingMessages(xs: immutable.Seq[ReceivedMessage]) extends State
}
