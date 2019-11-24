/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.kinesis.impl

import akka.NotUsed
import akka.annotation.InternalApi
import akka.dispatch.ExecutionContexts.sameThreadExecutionContext
import akka.stream.alpakka.kinesis.KinesisErrors.{ErrorPublishingRecords, FailurePublishingRecords}
import akka.stream.alpakka.kinesis.KinesisFlowSettings.{Exponential, Linear, RetryBackoffStrategy}
import akka.stream.stage._
import akka.stream.{Attributes, FlowShape, Inlet, Outlet}
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient
import software.amazon.awssdk.services.kinesis.model.{
  PutRecordsRequest,
  PutRecordsRequestEntry,
  PutRecordsResponse,
  PutRecordsResultEntry
}

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.immutable
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.Future
import scala.util.{Failure, Success, Try}

import scala.compat.java8.FutureConverters._

/**
 * Internal API
 *
 * @tparam T pass-through type
 */
@InternalApi
private[kinesis] final class KinesisFlowStage[T](
    streamName: String,
    maxRetries: Int,
    backoffStrategy: RetryBackoffStrategy,
    retryInitialTimeout: FiniteDuration
)(implicit kinesisClient: KinesisAsyncClient)
    extends GraphStage[
      FlowShape[immutable.Seq[(PutRecordsRequestEntry, T)], Future[immutable.Seq[(PutRecordsResultEntry, T)]]]
    ] {

  import KinesisFlowStage._

  private val in = Inlet[immutable.Seq[(PutRecordsRequestEntry, T)]]("KinesisFlowStage.in")
  private val out = Outlet[Future[immutable.Seq[(PutRecordsResultEntry, T)]]]("KinesisFlowStage.out")
  override val shape = FlowShape(in, out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new TimerGraphStageLogic(shape) with StageLogging with InHandler with OutHandler {

      type Token = Int
      type RetryCount = Int

      private var completionState: Option[Try[Unit]] = None

      private val pendingRequests: mutable.Queue[Job[T]] = mutable.Queue.empty
      private var inFlight: Int = 0
      private val putRecordsSuccessfulCallback: AsyncCallback[NotUsed] = getAsyncCallback(_ => putRecordsSuccessful())
      private val resendCallback: AsyncCallback[Result[T]] = getAsyncCallback(resend)
      private val failAfterResendsCallback: AsyncCallback[Result[T]] = getAsyncCallback(failAfterResends)

      private val waitingRetries: mutable.HashMap[Token, Job[T]] = mutable.HashMap.empty
      private var retryToken: Token = 0

      private def tryToExecute(): Unit =
        if (pendingRequests.nonEmpty && isAvailable(out)) {
          inFlight += 1
          val job = pendingRequests.dequeue()
          push(out, putRecords(job))
        }

      private def putRecords(job: Job[T]): Future[immutable.Seq[(PutRecordsResultEntry, T)]] = {

        val request = PutRecordsRequest
          .builder()
          .streamName(streamName)
          .records(job.records.map(_._1).asJavaCollection)
          .build()

        def handleResult(result: PutRecordsResponse): immutable.Seq[(PutRecordsResultEntry, T)] = {
          val correlatedRequestResult = result.records.asScala.zip(job.records).toList
          if (result.failedRecordCount > 0) {
            val result = Result(job.attempt,
                                correlatedRequestResult
                                  .filter {
                                    case (res, _) => res.errorCode != null
                                  })
            if (job.attempt > maxRetries) failAfterResendsCallback.invoke(result)
            else resendCallback.invoke(result)
          } else {
            putRecordsSuccessfulCallback.invoke(NotUsed)
          }

          correlatedRequestResult
            .filter {
              case (res, _) => res.errorCode == null
            }
            .map { case (res, (_, ctx)) => (res, ctx) }
        }

        def handleException(exception: Throwable): Throwable =
          FailurePublishingRecords(exception)

        kinesisClient
          .putRecords(request)
          .toScala
          .transform(handleResult, handleException)(sameThreadExecutionContext)

      }

      private def putRecordsSuccessful(): Unit = {
        inFlight -= 1
        tryToExecute()
        if (!hasBeenPulled(in)) tryPull(in)
        checkForCompletion()
      }

      private def resend(result: Result[T]): Unit = {
        log.debug("PutRecords call finished with partial errors; scheduling retry")
        inFlight -= 1
        waitingRetries.put(retryToken, Job(result.attempt + 1, result.recordsToRetry.map {
          case (_, reqCtx) => reqCtx
        }))
        scheduleOnce(
          retryToken,
          backoffStrategy match {
            case Exponential => retryInitialTimeout * scala.math.pow(2, result.attempt - 1).toInt
            case Linear => retryInitialTimeout * result.attempt
          }
        )
        retryToken += 1
      }

      private def failAfterResends(result: Result[T]): Unit = {
        log.debug("PutRecords call finished with partial errors after {} attempts", result.attempt)
        failStage(
          ErrorPublishingRecords(result.attempt, result.recordsToRetry.map { case (res, (_, ctx)) => (res, ctx) })
        )
      }

      private def checkForCompletion(): Unit =
        if (inFlight == 0 && pendingRequests.isEmpty && waitingRetries.isEmpty && isClosed(in)) {
          completionState match {
            case Some(Success(_)) => completeStage()
            case Some(Failure(ex)) => failStage(ex)
            case None => failStage(new IllegalStateException("Stage completed, but there is no info about status"))
          }
        }

      override protected def onTimer(timerKey: Any): Unit =
        waitingRetries.remove(timerKey.asInstanceOf[Token]) foreach { job =>
          log.debug("New PutRecords retry attempt available")
          pendingRequests.enqueue(job)
          tryToExecute()
        }

      override def postStop(): Unit = {
        pendingRequests.clear()
        waitingRetries.clear()
      }

      override def onUpstreamFinish(): Unit = {
        completionState = Some(Success(()))
        checkForCompletion()
      }

      override def onUpstreamFailure(ex: Throwable): Unit = {
        completionState = Some(Failure(ex))
        checkForCompletion()
      }

      override def onPull(): Unit = {
        tryToExecute()
        if (waitingRetries.isEmpty && !hasBeenPulled(in)) tryPull(in)
      }

      override def onPush(): Unit = {
        pendingRequests.enqueue(Job(1, grab(in)))
        tryToExecute()
      }

      setHandlers(in, out, this)
    }

}

/**
 * Internal API
 */
@InternalApi
private[kinesis] object KinesisFlowStage {

  private case class Result[T](attempt: Int,
                               recordsToRetry: immutable.Seq[(PutRecordsResultEntry, (PutRecordsRequestEntry, T))])
  private case class Job[T](attempt: Int, records: immutable.Seq[(PutRecordsRequestEntry, T)])

}
