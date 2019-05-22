/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.dynamodb.impl

import java.util.UUID

import akka.annotation.InternalApi
import akka.stream.alpakka.dynamodb.RetrySettings.{Exponential, Linear, RetryBackoffStrategy}
import akka.stream.alpakka.dynamodb.impl.RetryWithBackoff._
import akka.stream.impl.fusing.GraphStages.SimpleLinearGraphStage
import akka.stream.scaladsl.{Flow, Source}
import akka.stream.stage._
import akka.stream.{Attributes, Graph, SourceShape}

import scala.concurrent.duration.FiniteDuration

private[dynamodb] object RetryWithBackoff {
  case class RetryContainer(ex: Throwable, id: UUID = UUID.randomUUID)

  implicit class RecoverWithRetryImplicits[T, U, M](flow: Flow[T, U, M]) {
    def retryWithBackoff(maximumRetries: Int,
                         retryInitialTimeout: FiniteDuration,
                         backoffStrategy: RetryBackoffStrategy,
                         decider: PartialFunction[Throwable, Graph[SourceShape[U], M]]): Flow[T, U, M] =
      flow.via(new RetryWithBackoff(maximumRetries, retryInitialTimeout, backoffStrategy, decider))
  }
}

/**
 * INTERNAL API
 */
@InternalApi
private[dynamodb] final class RetryWithBackoff[T, M](val maximumRetries: Int,
                                                     val retryInitialTimeout: FiniteDuration,
                                                     val backoffStrategy: RetryBackoffStrategy,
                                                     val decider: PartialFunction[Throwable, Graph[SourceShape[T], M]])
    extends SimpleLinearGraphStage[T] {

  override def toString: String = "RetryWithBackoff"

  override def createLogic(attr: Attributes): GraphStageLogic = new TimerGraphStageLogic(shape) with StageLogging {
    var attempt = 0

    setHandler(
      in,
      new InHandler {
        override def onPush(): Unit = {
          val elem = grab(in)
          push(out, elem)
        }

        override def onUpstreamFailure(ex: Throwable): Unit = onFailure(ex)
      }
    )

    setHandler(out, new OutHandler {
      override def onPull(): Unit = pull(in)
    })

    override protected def onTimer(timerKey: Any): Unit = {
      val key = timerKey.asInstanceOf[RetryContainer]
      switchTo(key)
    }

    def onFailure(ex: Throwable): Unit =
      if ((maximumRetries < 0 || attempt < maximumRetries) && decider.isDefinedAt(ex)) {
        attempt += 1

        val delay =
          backoffStrategy match {
            case Exponential => retryInitialTimeout * Math.pow(2, attempt - 1).toInt
            case Linear => retryInitialTimeout * attempt
          }
        log.debug(s"Retry attempt $attempt for exception $ex to be scheduled after $delay ${delay.unit}")

        scheduleOnce(RetryContainer(ex), delay)
      } else {
        failStage(ex)
      }

    def switchTo(key: RetryContainer): Unit = {
      val sinkIn = new SubSinkInlet[T]("RetryWithBackoffSink")

      sinkIn.setHandler(new InHandler {
        override def onPush(): Unit = {
          val elem = sinkIn.grab()
          push(out, elem)
        }

        override def onUpstreamFinish(): Unit = completeStage()

        override def onUpstreamFailure(ex: Throwable): Unit = onFailure(ex)
      })

      val outHandler = new OutHandler {
        override def onPull(): Unit = sinkIn.pull()

        override def onDownstreamFinish(): Unit = sinkIn.cancel()
      }

      Source.fromGraph(decider(key.ex)).runWith(sinkIn.sink)(interpreter.subFusingMaterializer)
      setHandler(out, outHandler)
      if (isAvailable(out)) {
        sinkIn.pull()
      }
    }
  }
}
