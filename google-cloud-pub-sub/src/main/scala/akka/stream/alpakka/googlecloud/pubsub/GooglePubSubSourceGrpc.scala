/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.pubsub

import akka.stream.{Attributes, Outlet, SourceShape}
import akka.stream.stage._

import scala.util.{Failure, Success, Try}
import GooglePubSubSourceGrpc._
import com.google.pubsub.v1

import scala.collection.JavaConverters._
import scala.collection.immutable
import scala.concurrent.duration._

@akka.annotation.InternalApi
private final class GooglePubSubSourceGrpc(
    parallelism: Int,
    retryOnFailure: Boolean,
    maxConsecutiveFailures: Int,
    grpcApiFactory: GrpcApiFactory
) extends GraphStage[SourceShape[v1.ReceivedMessage]] {

  val out: Outlet[v1.ReceivedMessage] = Outlet("GooglePubSubSourceGrpc.out")
  override val shape: SourceShape[v1.ReceivedMessage] = SourceShape(out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new TimerGraphStageLogicWithLogging(shape) with OutHandler {
      private var state: State = Pending
      private var fetches: Int = 0
      private var consecutiveFailures: Int = 0

      def fetch(): Unit = {
        fetches += 1

        grpcApiFactory.get.read.onComplete { tr =>
          fetches -= 1
          callback.invoke(tr)
        }(materializer.executionContext)

        state = Fetching
      }

      private val callback = getAsyncCallback[Try[v1.PullResponse]] {
        case Failure(tr) =>
          consecutiveFailures += 1

          if (retryOnFailure && consecutiveFailures < maxConsecutiveFailures) {
            val delay = ((1d / 2) * (Math.pow(2, consecutiveFailures) - 1)).seconds
            log.warning(
              s"Fetching failed with error. Retrying in $delay. Consecutive failures: $consecutiveFailures, max: $maxConsecutiveFailures. Error: $tr"
            )
            scheduleOnce("Fetch", delay)
          } else {
            log.error(s"Fetching failed with error. Not retrying. Error: $tr")
            failStage(tr)
          }

        case Success(response) =>
          if (consecutiveFailures > 0) {
            log.info(s"After $consecutiveFailures consecutive failures, connection to PubSub has been re-established.")
            consecutiveFailures = 0
          }

          response.getReceivedMessagesList.asScala.toList match {
            case items @ head :: tail =>
              state match {
                case HoldingMessages(oldHead :: oldTail) =>
                  state = HoldingMessages(oldTail ::: items)
                  push(out, oldHead)
                case _ =>
                  state = HoldingMessages(tail)
                  push(out, head)
              }
            case Nil =>
              state = Fetching
              fetch()
          }
      }

      override def onTimer(timerKey: Any): Unit =
        timerKey match {
          case "Fetch" =>
            fetch()
        }

      override def onPull(): Unit =
        state match {
          case Pending =>
            fetch()

          case Fetching if fetches < parallelism =>
            fetch()

          case Fetching if fetches > parallelism =>
          // do nothing we will push on request result

          case HoldingMessages(xs) =>
            xs match {
              case head :: tail =>
                state = HoldingMessages(tail)
                push(out, head)
              case Nil =>
                fetch()
            }
        }

      setHandler(
        out,
        this
      )
    }
}

@akka.annotation.InternalApi
private object GooglePubSubSourceGrpc {
  private sealed trait State
  private case object Pending extends State
  private case object Fetching extends State
  private case class HoldingMessages(xs: immutable.List[v1.ReceivedMessage]) extends State
}
