/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.sqs.impl
import akka.annotation.InternalApi
import akka.stream.alpakka.sqs.MessageAction.Delete
import akka.stream.alpakka.sqs.{SqsAckResult, SqsBatchException}
import akka.stream.stage.{AsyncCallback, GraphStage, GraphStageLogic}
import akka.stream.{Attributes, FlowShape, Inlet, Outlet}
import com.amazonaws.handlers.AsyncHandler
import com.amazonaws.services.sqs.AmazonSQSAsync
import com.amazonaws.services.sqs.model.{
  DeleteMessageBatchRequest,
  DeleteMessageBatchRequestEntry,
  DeleteMessageBatchResult
}

import scala.collection.JavaConverters._
import scala.concurrent.{Future, Promise}

/**
 * INTERNAL API
 */
@InternalApi private[sqs] final class SqsBatchDeleteFlowStage(queueUrl: String, sqsClient: AmazonSQSAsync)
    extends GraphStage[FlowShape[Iterable[Delete], Future[List[SqsAckResult]]]] {

  private val in = Inlet[Iterable[Delete]]("actions")
  private val out = Outlet[Future[List[SqsAckResult]]]("results")
  override val shape = FlowShape(in, out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new SqsBatchStageLogic[Iterable[Delete]](shape) {
      private var deleteCallback: AsyncCallback[DeleteMessageBatchRequest] = _

      override def preStart(): Unit = {
        super.preStart()
        deleteCallback = getAsyncCallback[DeleteMessageBatchRequest] { request =>
          val entries = request.getEntries
          for (entry <- entries.asScala)
            log.debug("Deleted message {}", entry.getReceiptHandle)
          inFlight -= entries.size()
          checkForCompletion()
        }
      }

      override def onPush(): Unit = {
        val actionsIt = grab(in)
        val actions = actionsIt.toList
        val nrOfActions = actions.size
        val responsePromise = Promise[List[SqsAckResult]]
        inFlight += nrOfActions

        val request = new DeleteMessageBatchRequest(
          queueUrl,
          actions.zipWithIndex.map {
            case (action, index) =>
              new DeleteMessageBatchRequestEntry()
                .withReceiptHandle(action.message.getReceiptHandle)
                .withId(index.toString)
          }.asJava
        )
        val handler = new AsyncHandler[DeleteMessageBatchRequest, DeleteMessageBatchResult]() {
          override def onError(exception: Exception): Unit = {
            val batchException = new SqsBatchException(nrOfActions, exception)
            responsePromise.failure(batchException)
            failureCallback.invoke(batchException)
          }

          override def onSuccess(request: DeleteMessageBatchRequest, result: DeleteMessageBatchResult): Unit =
            if (!result.getFailed.isEmpty) {
              val nrOfFailedMessages = result.getFailed.size()
              val batchException: SqsBatchException =
                new SqsBatchException(
                  batchSize = nrOfActions,
                  cause = new Exception(
                    s"Some messages failed to delete. $nrOfFailedMessages of $nrOfActions messages failed"
                  )
                )
              responsePromise.failure(batchException)
              failureCallback.invoke(batchException)
            } else {
              responsePromise.success(actions.map(a => new SqsAckResult(Some(result), a)))
              deleteCallback.invoke(request)
            }

        }
        sqsClient.deleteMessageBatchAsync(
          request,
          handler
        )
        push(out, responsePromise.future)
      }
    }
}
