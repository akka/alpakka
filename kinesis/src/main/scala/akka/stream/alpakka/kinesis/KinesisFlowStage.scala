/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.kinesis

import akka.stream.alpakka.kinesis.KinesisErrors.{ErrorPublishingRecords, FailurePublishingRecords}
import akka.stream.alpakka.kinesis.KinesisFlowSettings.{Exponential, Lineal, RetryBackoffStrategy}
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
import scala.concurrent.duration._
import scala.concurrent.{Future, Promise}
import scala.util.{Failure, Success, Try}

import scala.language.postfixOps

private[kinesis] final class KinesisFlowStage(
    streamName: String,
    maxRetries: Int,
    backoffStrategy: RetryBackoffStrategy,
    retryInitialTimeout: FiniteDuration
)(implicit kinesisClient: AmazonKinesisAsync)
    extends GraphStage[FlowShape[Seq[PutRecordsRequestEntry], Future[PutRecordsResult]]] {

  private val in = Inlet[Seq[PutRecordsRequestEntry]]("KinesisFlowStage.in")
  private val out = Outlet[Future[PutRecordsResult]]("KinesisFlowStage.out")
  override val shape = FlowShape(in, out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new TimerGraphStageLogic(shape) with StageLogging with InHandler with OutHandler {

      private val retryBaseInMillis = retryInitialTimeout.toMillis

      private var completionState: Option[Try[Unit]] = None

      private val pendingRequests: mutable.Queue[(Int, Seq[PutRecordsRequestEntry])] = mutable.Queue.empty
      private var resultCallback: AsyncCallback[Result] = _
      private var inFlight = 0

      private var retryToken = 0
      private val waitingRetries: mutable.HashMap[Int, Result] = mutable.HashMap.empty

      private def tryToExecute() =
        if (pendingRequests.nonEmpty && isAvailable(out)) {
          log.debug("Executing PutRecords call")
          inFlight += 1
          val (attempt, records) = pendingRequests.dequeue()
          push(
            out,
            putRecords(
              streamName,
              records,
              recordsToRetry => resultCallback.invoke(Result(attempt, recordsToRetry))
            )
          )
        }

      private def handleResult(result: Result): Unit = result match {
        case Result(_, Nil) =>
          log.debug("PutRecords call finished successfully")
          inFlight -= 1
          tryToExecute()
          if (!hasBeenPulled(in)) tryPull(in)
          checkForCompletion()
        case Result(attempt, errors) if attempt > maxRetries =>
          log.debug("PutRecords call finished with partial errors after {} attempts", attempt)
          failStage(ErrorPublishingRecords(attempt, errors.map(_._1)))
        case x @ Result(attempt, _) =>
          log.debug("PutRecords call finished with partial errors; scheduling retry")
          inFlight -= 1
          waitingRetries.put(retryToken, x)
          scheduleOnce(retryToken, backoffStrategy match {
            case Exponential => scala.math.pow(retryBaseInMillis, attempt).toInt millis
            case Lineal => retryInitialTimeout * attempt
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
        waitingRetries.remove(timerKey.asInstanceOf[Int]) foreach { result =>
          log.debug("New PutRecords retry attempt available")
          pendingRequests.enqueue(result.attempt + 1 -> result.recordsToRetry.map(_._2))
          tryToExecute()
        }

      override def preStart() = {
        pull(in)
        resultCallback = getAsyncCallback[Result](handleResult)
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
        pendingRequests.enqueue(1 -> grab(in))
        tryToExecute()
      }

      setHandlers(in, out, this)
    }

}

object KinesisFlowStage {

  private def putRecords(
      streamName: String,
      recordEntries: Seq[PutRecordsRequestEntry],
      retryRecordsCallback: Seq[(PutRecordsResultEntry, PutRecordsRequestEntry)] => Unit
  )(implicit kinesisClient: AmazonKinesisAsync): Future[PutRecordsResult] = {

    val p = Promise[PutRecordsResult]

    kinesisClient
      .putRecordsAsync(
        new PutRecordsRequest()
          .withStreamName(streamName)
          .withRecords(recordEntries.asJavaCollection),
        new AsyncHandler[PutRecordsRequest, PutRecordsResult] {

          override def onError(exception: Exception): Unit =
            p.failure(FailurePublishingRecords(exception))

          override def onSuccess(request: PutRecordsRequest, result: PutRecordsResult): Unit = {
            if (result.getFailedRecordCount > 0) {
              retryRecordsCallback(
                result.getRecords.asScala
                  .zip(request.getRecords.asScala)
                  .filter(_._1.getErrorCode != null)
              )
            } else {
              retryRecordsCallback(Nil)
            }
            p.success(result)
          }
        }
      )

    p.future
  }

  private case class Result(attempt: Int, recordsToRetry: Seq[(PutRecordsResultEntry, PutRecordsRequestEntry)])

}
