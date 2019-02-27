/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.kinesisfirehose.impl

import akka.annotation.InternalApi
import akka.stream.alpakka.kinesisfirehose.KinesisFirehoseErrors.{ErrorPublishingRecords, FailurePublishingRecords}
import akka.stream.alpakka.kinesisfirehose.KinesisFirehoseFlowSettings.{Exponential, Linear, RetryBackoffStrategy}
import akka.stream.stage._
import akka.stream.{Attributes, FlowShape, Inlet, Outlet}
import com.amazonaws.handlers.AsyncHandler
import com.amazonaws.services.kinesisfirehose.AmazonKinesisFirehoseAsync
import com.amazonaws.services.kinesisfirehose.model.{
  PutRecordBatchRequest,
  PutRecordBatchResponseEntry,
  PutRecordBatchResult,
  Record
}

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{Future, Promise}
import scala.util.{Failure, Success, Try}

/**
 * Internal API
 */
@InternalApi
private[kinesisfirehose] final class KinesisFirehoseFlowStage(
    streamName: String,
    maxRetries: Int,
    backoffStrategy: RetryBackoffStrategy,
    retryInitialTimeout: FiniteDuration
)(implicit kinesisClient: AmazonKinesisFirehoseAsync)
    extends GraphStage[FlowShape[Seq[Record], Future[PutRecordBatchResult]]] {

  import KinesisFirehoseFlowStage._

  private val in = Inlet[Seq[Record]]("KinesisFirehoseFlowStage.in")
  private val out = Outlet[Future[PutRecordBatchResult]]("KinesisFirehoseFlowStage.out")
  override val shape = FlowShape(in, out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new TimerGraphStageLogic(shape) with StageLogging with InHandler with OutHandler {

      type Token = Int
      type RetryCount = Int

      private var completionState: Option[Try[Unit]] = _

      private val pendingRequests: mutable.Queue[Job] = mutable.Queue.empty
      private var resultCallback: AsyncCallback[Result] = _
      private var inFlight: Int = _

      private val waitingRetries: mutable.HashMap[Token, Job] = mutable.HashMap.empty
      private var retryToken: Token = _

      private def tryToExecute() =
        if (pendingRequests.nonEmpty && isAvailable(out)) {
          log.debug("Executing PutRecordBatch call")
          inFlight += 1
          val job = pendingRequests.dequeue()
          push(
            out,
            putRecordBatch(
              streamName,
              job.records,
              recordsToRetry => resultCallback.invoke(Result(job.attempt, recordsToRetry))
            )
          )
        }

      private def handleResult(result: Result): Unit = result match {
        case Result(_, Nil) =>
          log.debug("PutRecordBatch call finished successfully")
          inFlight -= 1
          tryToExecute()
          if (!hasBeenPulled(in)) tryPull(in)
          checkForCompletion()
        case Result(attempt, errors) if attempt > maxRetries =>
          log.debug("PutRecordBatch call finished with partial errors after {} attempts", attempt)
          failStage(ErrorPublishingRecords(attempt, errors.map(_._1)))
        case Result(attempt, errors) =>
          log.debug("PutRecordBatch call finished with partial errors; scheduling retry")
          inFlight -= 1
          waitingRetries.put(retryToken, Job(attempt + 1, errors.map(_._2)))
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
          log.debug("New PutRecordBatch retry attempt available")
          pendingRequests.enqueue(job)
          tryToExecute()
        }

      override def preStart() = {
        completionState = None
        inFlight = 0
        retryToken = 0
        resultCallback = getAsyncCallback[Result](handleResult)
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
        log.debug("New PutRecordBatch request available")
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
private[kinesisfirehose] object KinesisFirehoseFlowStage {
  private def putRecordBatch(
      streamName: String,
      recordEntries: Seq[Record],
      retryRecordsCallback: Seq[(PutRecordBatchResponseEntry, Record)] => Unit
  )(implicit kinesisClient: AmazonKinesisFirehoseAsync): Future[PutRecordBatchResult] = {

    val p = Promise[PutRecordBatchResult]

    kinesisClient
      .putRecordBatchAsync(
        new PutRecordBatchRequest()
          .withDeliveryStreamName(streamName)
          .withRecords(recordEntries.asJavaCollection),
        new AsyncHandler[PutRecordBatchRequest, PutRecordBatchResult] {

          override def onError(exception: Exception): Unit =
            p.failure(FailurePublishingRecords(exception))

          override def onSuccess(request: PutRecordBatchRequest, result: PutRecordBatchResult): Unit = {
            if (result.getFailedPutCount > 0) {
              retryRecordsCallback(
                result.getRequestResponses.asScala
                  .zip(request.getRecords.asScala)
                  .filter(_._1.getErrorCode != null)
                  .toIndexedSeq
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

  private case class Result(attempt: Int, recordsToRetry: Seq[(PutRecordBatchResponseEntry, Record)])
  private case class Job(attempt: Int, records: Seq[Record])
}
