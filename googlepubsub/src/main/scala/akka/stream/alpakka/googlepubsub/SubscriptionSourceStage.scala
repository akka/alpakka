/*
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.googlepubsub

import akka.actor.ActorSystem
import akka.stream.{Attributes, Materializer, Outlet, SourceShape}
import akka.stream.stage.{GraphStage, GraphStageLogic, OutHandler}

import scala.util.Try

final class SubscriptionSourceStage(projectId: String,
                                    apiKey: String,
                                    session: Session,
                                    subscription: String,
                                    httpApi: HttpApi)(implicit as: ActorSystem)
    extends GraphStage[SourceShape[ReceivedMessage]] {

  val out: Outlet[ReceivedMessage] = Outlet("GooglePubSub.out")
  override val shape: SourceShape[ReceivedMessage] = SourceShape(out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) {
      private var state: State = Pending

      sealed trait State
      case object Pending extends State
      case object Fetching extends State
      case class HoldingMessages(xs: Seq[ReceivedMessage]) extends State

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
        case (tr, mat) =>
          tr match {
            case util.Failure(th) =>
              failStage(th)
            case util.Success(response) =>
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
      }

      setHandler(out,
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
      })
    }
}
