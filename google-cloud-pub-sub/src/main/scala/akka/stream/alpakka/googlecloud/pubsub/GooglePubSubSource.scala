/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.googlecloud.pubsub

import akka.actor.ActorSystem
import akka.stream.{Attributes, Materializer, Outlet, SourceShape}
import akka.stream.stage.{GraphStage, GraphStageLogic, OutHandler}

import scala.util.{Failure, Success, Try}
import GooglePubSubSource._

import scala.collection.immutable

@akka.annotation.InternalApi
private final class GooglePubSubSource(projectId: String,
                                       apiKey: String,
                                       session: Session,
                                       subscription: String,
                                       httpApi: HttpApi)(implicit as: ActorSystem)
    extends GraphStage[SourceShape[ReceivedMessage]] {

  val out: Outlet[ReceivedMessage] = Outlet("GooglePubSubSource.out")
  override val shape: SourceShape[ReceivedMessage] = SourceShape(out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) {
      private var state: State = Pending

      def fetch(implicit mat: Materializer): Unit = {
        import mat.executionContext

        val req = session.getToken().flatMap { token =>
          httpApi.pull(project = projectId, subscription = subscription, accessToken = token, apiKey = apiKey)
        }
        req.onComplete { tr =>
          callback.invoke(tr -> mat)
        }
        state = Fetching
      }

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

@akka.annotation.InternalApi
private object GooglePubSubSource {
  private sealed trait State
  private case object Pending extends State
  private case object Fetching extends State
  private case class HoldingMessages(xs: immutable.Seq[ReceivedMessage]) extends State
}
