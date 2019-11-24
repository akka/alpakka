/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.kinesisfirehose.impl

import akka.annotation.InternalApi
import akka.dispatch.ExecutionContexts.sameThreadExecutionContext
import akka.stream.alpakka.kinesisfirehose.KinesisFirehoseErrors.{ErrorPublishingRecords, FailurePublishingRecords}
import akka.stream.alpakka.kinesisfirehose.KinesisFirehoseFlowSettings.{Exponential, Linear, RetryBackoffStrategy}
import akka.stream.stage._
import akka.stream.{Attributes, FlowShape, Inlet, Outlet}
import software.amazon.awssdk.services.firehose.FirehoseAsyncClient
import software.amazon.awssdk.services.firehose.model.{
  PutRecordBatchRequest,
  PutRecordBatchResponse,
  PutRecordBatchResponseEntry,
  Record
}

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.Future
import scala.util.{Failure, Success, Try}

import scala.compat.java8.FutureConverters._

/**
 * Internal API
 */
@InternalApi
private[kinesisfirehose] final class KinesisFirehoseFlowStage(
    streamName: String,
    maxRetries: Int,
    backoffStrategy: RetryBackoffStrategy,
    retryInitialTimeout: FiniteDuration
)(implicit kinesisClient: FirehoseAsyncClient)
    extends GraphStage[FlowShape[Seq[Record], Future[PutRecordBatchResponse]]] {

  import KinesisFirehoseFlowStage._

  private val in = Inlet[Seq[Record]]("KinesisFirehoseFlowStage.in")
  private val out = Outlet[Future[PutRecordBatchResponse]]("KinesisFirehoseFlowStage.out")
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
  )(implicit kinesisClient: FirehoseAsyncClient): Future[PutRecordBatchResponse] = {

    val request = PutRecordBatchRequest
      .builder()
      .deliveryStreamName(streamName)
      .records(recordEntries.asJavaCollection)
      .build()

    val handlePutRecordBatch: Try[PutRecordBatchResponse] => Try[PutRecordBatchResponse] = {
      case Failure(exception) => Failure(FailurePublishingRecords(exception))
      case Success(result) =>
        if (result.failedPutCount > 0) {
          retryRecordsCallback(
            result.requestResponses.asScala
              .zip(request.records.asScala)
              .filter(_._1.errorCode != null)
              .toIndexedSeq
          )
        } else {
          retryRecordsCallback(Nil)
        }
        Success(result)
    }

    kinesisClient
      .putRecordBatch(request)
      .toScala
      .transform(handlePutRecordBatch)(sameThreadExecutionContext)
  }

  private case class Result(attempt: Int, recordsToRetry: Seq[(PutRecordBatchResponseEntry, Record)])
  private case class Job(attempt: Int, records: Seq[Record])
}
