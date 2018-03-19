/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.sqs

import akka.stream.alpakka.sqs.scaladsl.AckResult
import akka.stream.stage._
import akka.stream._
import com.amazonaws.handlers.AsyncHandler
import com.amazonaws.services.sqs.AmazonSQSAsync
import com.amazonaws.services.sqs.model._

import scala.collection.JavaConverters._
import scala.concurrent.{Future, Promise}
import scala.util.{Failure, Success, Try}

private[sqs] final class SqsBatchDeleteFlowStage(queueUrl: String, sqsClient: AmazonSQSAsync)
    extends GraphStage[FlowShape[Iterable[Message], Future[List[AckResult]]]] {
  private val in = Inlet[Iterable[Message]]("messages")
  private val out = Outlet[Future[List[AckResult]]]("results")
  override val shape = FlowShape(in, out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new GenericStage[Iterable[Message]](shape) {
      private def handleDelete(request: DeleteMessageBatchRequest): Unit = {
        val entries = request.getEntries
        for (entry <- entries.asScala)
          log.debug(s"Deleted message {}", entry.getReceiptHandle)
        inFlight -= entries.size()
        checkForCompletion()
      }
      private var deleteCallback: AsyncCallback[DeleteMessageBatchRequest] = _
      override def preStart(): Unit = {
        super.preStart()
        deleteCallback = getAsyncCallback[DeleteMessageBatchRequest](handleDelete)
      }
      override def onPush(): Unit = {
        val messagesIt = grab(in)
        val messages = messagesIt.toList
        val nrOfMessages = messages.size
        val responsePromise = Promise[List[AckResult]]
        inFlight += nrOfMessages

        sqsClient.deleteMessageBatchAsync(
          new DeleteMessageBatchRequest(
            queueUrl,
            messages.zipWithIndex.map {
              case (message, index) =>
                new DeleteMessageBatchRequestEntry().withReceiptHandle(message.getReceiptHandle).withId(index.toString)
            }.asJava
          ),
          new AsyncHandler[DeleteMessageBatchRequest, DeleteMessageBatchResult]() {
            override def onError(exception: Exception): Unit = {
              val batchException = BatchException(messages.size, exception)
              responsePromise.failure(batchException)
              failureCallback.invoke(batchException)
            }

            override def onSuccess(request: DeleteMessageBatchRequest, result: DeleteMessageBatchResult): Unit =
              if (!result.getFailed.isEmpty) {
                val nrOfFailedMessages = result.getFailed.size()
                val batchException: BatchException =
                  BatchException(
                    batchSize = nrOfMessages,
                    cause = new Exception(
                      s"Some messages failed to delete. $nrOfFailedMessages of $nrOfMessages messages failed"
                    )
                  )
                responsePromise.failure(batchException)
                failureCallback.invoke(batchException)
              } else {
                responsePromise.success(messages.map(msg => AckResult(Some(result), msg.getBody)))
                deleteCallback.invoke(request)
              }

          }
        )
        push(out, responsePromise.future)
      }
    }
}

private[sqs] final class SqsBatchChangeMessageVisibilityFlowStage(queueUrl: String, sqsClient: AmazonSQSAsync)
    extends GraphStage[FlowShape[Iterable[(Message, MessageAction.ChangeMessageVisibility)], Future[List[AckResult]]]] {
  private val in = Inlet[Iterable[(Message, MessageAction.ChangeMessageVisibility)]]("messages")
  private val out = Outlet[Future[List[AckResult]]]("results")
  override val shape = FlowShape(in, out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new GenericStage(shape) {
      private def handleChangeVisibility(request: ChangeMessageVisibilityBatchRequest): Unit = {
        val entries = request.getEntries
        for (entry <- entries.asScala)
          log.debug(s"Set visibility timeout for message {} to {}", entry.getReceiptHandle, entry.getVisibilityTimeout)
        inFlight -= entries.size()
        checkForCompletion()
      }
      private var changeVisibilityCallback: AsyncCallback[ChangeMessageVisibilityBatchRequest] = _
      override def preStart(): Unit = {
        super.preStart()
        changeVisibilityCallback = getAsyncCallback[ChangeMessageVisibilityBatchRequest](handleChangeVisibility)
      }
      override def onPush() = {
        val messagesIt = grab(in)
        val messages = messagesIt.toList
        val nrOfMessages = messages.size
        val responsePromise = Promise[List[AckResult]]
        inFlight += nrOfMessages

        sqsClient
          .changeMessageVisibilityBatchAsync(
            new ChangeMessageVisibilityBatchRequest(
              queueUrl,
              messages.zipWithIndex.map {
                case ((message, MessageAction.ChangeMessageVisibility(visibilityTimeout)), index) =>
                  new ChangeMessageVisibilityBatchRequestEntry()
                    .withReceiptHandle(message.getReceiptHandle)
                    .withVisibilityTimeout(visibilityTimeout)
                    .withId(index.toString)
              }.asJava
            ),
            new AsyncHandler[ChangeMessageVisibilityBatchRequest, ChangeMessageVisibilityBatchResult]() {
              override def onError(exception: Exception): Unit = {
                val batchException = BatchException(messages.size, exception)
                responsePromise.failure(batchException)
                failureCallback.invoke(batchException)
              }

              override def onSuccess(request: ChangeMessageVisibilityBatchRequest,
                                     result: ChangeMessageVisibilityBatchResult): Unit =
                if (!result.getFailed.isEmpty) {
                  val nrOfFailedMessages = result.getFailed.size()
                  val batchException: BatchException =
                    BatchException(
                      batchSize = nrOfMessages,
                      cause = new Exception(
                        s"Some messages failed to change visibility. $nrOfFailedMessages of $nrOfMessages messages failed"
                      )
                    )
                  responsePromise.failure(batchException)
                  failureCallback.invoke(batchException)
                } else {
                  responsePromise.success(messages.map(msg => AckResult(Some(result), msg._1.getBody)))
                  changeVisibilityCallback.invoke(request)
                }
            }
          )
        push(out, responsePromise.future)
      }
    }
}

private abstract class GenericStage[A](shape: FlowShape[A, Future[scala.List[AckResult]]])
    extends GraphStageLogic(shape)
    with InHandler
    with OutHandler
    with StageLogging {
  import shape._

  protected var inFlight = 0

  private var completionState: Option[Try[Unit]] = None

  private def handleFailure(exception: BatchException): Unit = {
    log.error(exception, "Client failure: {}", exception)
    inFlight -= exception.batchSize
    failStage(exception)
  }

  protected var failureCallback: AsyncCallback[BatchException] = _

  override def preStart(): Unit = {
    super.preStart()
    failureCallback = getAsyncCallback[BatchException](handleFailure)
  }

  override protected def logSource: Class[_] = classOf[GenericStage[A]]

  def checkForCompletion() =
    if (isClosed(in) && inFlight == 0) {
      completionState match {
        case Some(Success(_)) => completeStage()
        case Some(Failure(ex)) => failStage(ex)
        case None => failStage(new IllegalStateException("Stage completed, but there is no info about status"))
      }
    }

  override def onPull() =
    tryPull(in)

  override def onUpstreamFinish() = {
    completionState = Some(Success(()))
    checkForCompletion()
  }

  override def onUpstreamFailure(ex: Throwable) = {
    completionState = Some(Failure(ex))
    checkForCompletion()
  }

  setHandlers(in, out, this)
}
