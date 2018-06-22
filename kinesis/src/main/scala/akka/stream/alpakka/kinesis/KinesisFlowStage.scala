/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.kinesis

import akka.stream.alpakka.kinesis.KinesisErrors.{ErrorPublishingRecords, FailurePublishingRecords}
import akka.stream.alpakka.kinesis.KinesisFlowSettings.{Exponential, Linear, RetryBackoffStrategy}
import akka.stream.alpakka.kinesis.KinesisFlowStage._
import akka.stream.stage._
import akka.stream.{Attributes, FlowShape, Inlet, Outlet}
import com.amazonaws.handlers.AsyncHandler
import com.amazonaws.services.kinesis.AmazonKinesisAsync
import com.amazonaws.services.kinesis.model.{
  PutRecordsRequest,
  PutRecordsRequestEntry,
  PutRecordsResult,
  PutRecordsResultEntry
}

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{Future, Promise}
import scala.util.{Failure, Success, Try}

import scala.language.postfixOps

private[kinesis] final class KinesisFlowStage[T](
    streamName: String,
    maxRetries: Int,
    backoffStrategy: RetryBackoffStrategy,
    retryInitialTimeout: FiniteDuration
)(implicit kinesisClient: AmazonKinesisAsync)
    extends GraphStage[FlowShape[Seq[(PutRecordsRequestEntry, T)], Future[Seq[(PutRecordsResultEntry, T)]]]] {

  private val in = Inlet[Seq[(PutRecordsRequestEntry, T)]]("KinesisFlowStage.in")
  private val out = Outlet[Future[Seq[(PutRecordsResultEntry, T)]]]("KinesisFlowStage.out")
  override val shape = FlowShape(in, out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new TimerGraphStageLogic(shape) with StageLogging with InHandler with OutHandler {

      type Token = Int
      type RetryCount = Int

      private var completionState: Option[Try[Unit]] = _

      private val pendingRequests: mutable.Queue[Job[T]] = mutable.Queue.empty
      private var resultCallback: AsyncCallback[Result[T]] = _
      private var inFlight: Int = _

      private val waitingRetries: mutable.HashMap[Token, Job[T]] = mutable.HashMap.empty
      private var retryToken: Token = _

      private def tryToExecute() =
        if (pendingRequests.nonEmpty && isAvailable(out)) {
          log.debug("Executing PutRecords call")
          inFlight += 1
          val job = pendingRequests.dequeue()
          push(
            out,
            putRecords[T](
              streamName,
              job.records,
              recordsToRetry => resultCallback.invoke(Result(job.attempt, recordsToRetry))
            )
          )
        }

      private def handleResult(result: Result[T]): Unit = result match {
        case Result(_, Nil) =>
          log.debug("PutRecords call finished successfully")
          inFlight -= 1
          tryToExecute()
          if (!hasBeenPulled(in)) tryPull(in)
          checkForCompletion()
        case Result(attempt, errors) if attempt > maxRetries =>
          log.debug("PutRecords call finished with partial errors after {} attempts", attempt)
          failStage(ErrorPublishingRecords(attempt, errors.map({ case (_, res, ctx) => (res, ctx) })))
        case Result(attempt, errors) =>
          log.debug("PutRecords call finished with partial errors; scheduling retry")
          inFlight -= 1
          waitingRetries.put(retryToken, Job(attempt + 1, errors.map({ case (req, _, ctx) => (req, ctx) })))
          scheduleOnce(retryToken, backoffStrategy match {
            case Exponential => retryInitialTimeout * scala.math.pow(2, attempt - 1).toInt
            case Linear => retryInitialTimeout * attempt
          })
          retryToken += 1
      }

      private def checkForCompletion() =
        if (inFlight == 0 && pendingRequests.isEmpty && waitingRetries.isEmpty && isClosed(in)) {
          completionState match {
            case Some(Success(_)) => completeStage()
            case Some(Failure(ex)) => failStage(ex)
            case None => failStage(new IllegalStateException("Stage completed, but there is no info about status"))
          }
        }

      override protected def onTimer(timerKey: Any) =
        waitingRetries.remove(timerKey.asInstanceOf[Token]) foreach { job =>
          log.debug("New PutRecords retry attempt available")
          pendingRequests.enqueue(job)
          tryToExecute()
        }

      override def preStart() = {
        completionState = None
        inFlight = 0
        retryToken = 0
        resultCallback = getAsyncCallback[Result[T]](handleResult)
        pull(in)
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
        log.debug("New PutRecords request available")
        pendingRequests.enqueue(Job(1, grab(in)))
        tryToExecute()
      }

      setHandlers(in, out, this)
    }

}

object KinesisFlowStage {

  private def putRecords[T](
      streamName: String,
      recordEntries: Seq[(PutRecordsRequestEntry, T)],
      retryRecordsCallback: Seq[(PutRecordsRequestEntry, PutRecordsResultEntry, T)] => Unit
  )(implicit kinesisClient: AmazonKinesisAsync): Future[Seq[(PutRecordsResultEntry, T)]] = {

    val p = Promise[Seq[(PutRecordsResultEntry, T)]]

    kinesisClient
      .putRecordsAsync(
        new PutRecordsRequest()
          .withStreamName(streamName)
          .withRecords(recordEntries.map(_._1).asJavaCollection),
        new AsyncHandler[PutRecordsRequest, PutRecordsResult] {

          override def onError(exception: Exception): Unit =
            p.failure(FailurePublishingRecords(exception))

          override def onSuccess(request: PutRecordsRequest, result: PutRecordsResult): Unit = {
            val correlatedRequestResult = result.getRecords.asScala
              .zip(recordEntries)
              .map({ case (res, (req, ctx)) => (req, res, ctx) })
            if (result.getFailedRecordCount > 0) {
              retryRecordsCallback(
                correlatedRequestResult
                  .filter(_._2.getErrorCode != null)
              )
            } else {
              retryRecordsCallback(Nil)
            }
            p.success(
              correlatedRequestResult
                .filter(_._2.getErrorCode == null)
                .map({ case (_, res, ctx) => (res, ctx) })
            )
          }
        }
      )

    p.future
  }

  private case class Result[T](attempt: Int, recordsToRetry: Seq[(PutRecordsRequestEntry, PutRecordsResultEntry, T)])
  private case class Job[T](attempt: Int, records: Seq[(PutRecordsRequestEntry, T)])

}
