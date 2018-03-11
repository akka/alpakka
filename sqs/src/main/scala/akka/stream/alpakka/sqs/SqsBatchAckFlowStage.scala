/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.sqs

import akka.stream.alpakka.sqs.SqsBatchAckFlowStage._
import akka.stream.alpakka.sqs.scaladsl.AckResult
import akka.stream.stage._
import akka.stream.{Attributes, FlowShape, Inlet, Outlet}
import com.amazonaws.handlers.AsyncHandler
import com.amazonaws.services.sqs.AmazonSQSAsync
import com.amazonaws.services.sqs.model._

import scala.collection.JavaConverters._
import scala.concurrent.{Future, Promise}
import scala.util.{Failure, Success, Try}

private[sqs] final class SqsBatchAckFlowStage[A](queueUrl: String, sqsClient: AmazonSQSAsync)(
    implicit
    body: A => String,
    deleteMessageEntry: A => DeleteMessageBatchRequestEntry,
    changeVisibilityEntry: A => ChangeMessageVisibilityBatchRequestEntry
) extends GraphStage[FlowShape[(Iterable[A], Call), Future[List[AckResult]]]] {

  private val in = Inlet[(Iterable[A], Call)]("messages")
  private val out = Outlet[Future[List[AckResult]]]("results")
  override val shape = FlowShape(in, out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) with StageLogging {
      private var inFlight = 0

      var completionState: Option[Try[Unit]] = None

      private def handleFailure(exception: BatchException): Unit = {
        log.error(exception, "Client failure: {}", exception)
        inFlight -= exception.batchSize
        failStage(exception)
      }

      private def handleChangeVisibility(request: ChangeMessageVisibilityBatchRequest): Unit = {
        val entries = request.getEntries
        for (entry <- entries.asScala)
          log.debug(s"Set visibility timeout for message {} to {}", entry.getReceiptHandle, entry.getVisibilityTimeout)
        inFlight -= entries.size()
        checkForCompletion()
      }
      private def handleDelete(request: DeleteMessageBatchRequest): Unit = {
        val entries = request.getEntries
        for (entry <- entries.asScala)
          log.debug(s"Deleted message {}", entry.getReceiptHandle)
        inFlight -= entries.size()
        checkForCompletion()
      }

      var failureCallback: AsyncCallback[BatchException] = _
      var changeVisibilityCallback: AsyncCallback[ChangeMessageVisibilityBatchRequest] = _
      var deleteCallback: AsyncCallback[DeleteMessageBatchRequest] = _

      override def preStart(): Unit = {
        super.preStart()
        failureCallback = getAsyncCallback[BatchException](handleFailure)
        changeVisibilityCallback = getAsyncCallback[ChangeMessageVisibilityBatchRequest](handleChangeVisibility)
        deleteCallback = getAsyncCallback[DeleteMessageBatchRequest](handleDelete)
      }

      override protected def logSource: Class[_] = classOf[SqsBatchAckFlowStage[A]]

      def checkForCompletion() =
        if (isClosed(in) && inFlight == 0) {
          completionState match {
            case Some(Success(_)) => completeStage()
            case Some(Failure(ex)) => failStage(ex)
            case None => failStage(new IllegalStateException("Stage completed, but there is no info about status"))
          }
        }

      setHandler(out, new OutHandler {
        override def onPull() =
          tryPull(in)
      })

      setHandler(
        in,
        new InHandler {

          override def onUpstreamFinish() = {
            completionState = Some(Success(()))
            checkForCompletion()
          }

          override def onUpstreamFailure(ex: Throwable) = {
            completionState = Some(Failure(ex))
            checkForCompletion()
          }

          override def onPush() = {
            val (messagesIt, action) = grab(in)
            val messages = messagesIt.toList
            val nrOfMessages = messages.size
            val responsePromise = Promise[List[AckResult]]
            action match {
              case SqsBatchAckFlowStage.Delete =>
                inFlight += nrOfMessages

                sqsClient.deleteMessageBatchAsync(
                  new DeleteMessageBatchRequest(queueUrl, messages.zipWithIndex.map {
                    case (message, index) =>
                      deleteMessageEntry(message).withId(index.toString)
                  }.asJava),
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
                        responsePromise.success(messages.map(msg => AckResult(Some(result), body(msg))))
                        deleteCallback.invoke(request)
                      }

                  }
                )
              case SqsBatchAckFlowStage.ChangeMessageVisibility =>
                inFlight += nrOfMessages

                sqsClient
                  .changeMessageVisibilityBatchAsync(
                    new ChangeMessageVisibilityBatchRequest(queueUrl, messages.zipWithIndex.map {
                      case (message, index) =>
                        changeVisibilityEntry(message).withId(index.toString)
                    }.asJava),
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
                          responsePromise.success(messages.map(msg => AckResult(Some(result), body(msg))))
                          changeVisibilityCallback.invoke(request)
                        }
                    }
                  )
              case SqsBatchAckFlowStage.Ignore =>
                responsePromise.success(messages.map(msg => AckResult(None, body(msg))))
            }
            push(out, responsePromise.future)
          }
        }
      )
    }
}

private[sqs] object SqsBatchAckFlowStage {

  private[sqs] sealed trait Call
  private[sqs] case object Delete extends Call
  private[sqs] case object Ignore extends Call
  private[sqs] case object ChangeMessageVisibility extends Call

}
