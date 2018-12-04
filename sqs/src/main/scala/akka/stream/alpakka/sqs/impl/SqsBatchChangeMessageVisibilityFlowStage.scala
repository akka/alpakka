/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.sqs.impl

import akka.annotation.InternalApi
import akka.stream.alpakka.sqs.{MessageAction, SqsAckResult, SqsBatchException}
import akka.stream.stage.{AsyncCallback, GraphStage, GraphStageLogic}
import akka.stream.{Attributes, FlowShape, Inlet, Outlet}
import com.amazonaws.handlers.AsyncHandler
import com.amazonaws.services.sqs.AmazonSQSAsync
import com.amazonaws.services.sqs.model.{
  ChangeMessageVisibilityBatchRequest,
  ChangeMessageVisibilityBatchRequestEntry,
  ChangeMessageVisibilityBatchResult
}

import scala.collection.JavaConverters._
import scala.concurrent.{Future, Promise}

/**
 * INTERNAL API
 */
@InternalApi private[sqs] final class SqsBatchChangeMessageVisibilityFlowStage(queueUrl: String,
                                                                               sqsClient: AmazonSQSAsync)
    extends GraphStage[FlowShape[Iterable[MessageAction.ChangeMessageVisibility], Future[List[SqsAckResult]]]] {

  private val in = Inlet[Iterable[MessageAction.ChangeMessageVisibility]]("actions")
  private val out = Outlet[Future[List[SqsAckResult]]]("results")
  override val shape = FlowShape(in, out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new SqsBatchStageLogic(shape) {

      private var changeVisibilityCallback: AsyncCallback[ChangeMessageVisibilityBatchRequest] = _

      override def preStart(): Unit = {
        super.preStart()
        changeVisibilityCallback = getAsyncCallback[ChangeMessageVisibilityBatchRequest] { request =>
          val entries = request.getEntries
          for (entry <- entries.asScala)
            log.debug("Set visibility timeout for message {} to {}", entry.getReceiptHandle, entry.getVisibilityTimeout)
          inFlight -= entries.size()
          checkForCompletion()
        }
      }

      override def onPush(): Unit = {
        val actions = grab(in).toList
        val nrOfActions = actions.size

        val responsePromise = Promise[List[SqsAckResult]]
        inFlight += nrOfActions

        val handler =
          new AsyncHandler[ChangeMessageVisibilityBatchRequest, ChangeMessageVisibilityBatchResult]() {
            override def onError(exception: Exception): Unit = {
              val batchException = new SqsBatchException(nrOfActions, exception)
              responsePromise.failure(batchException)
              failureCallback.invoke(batchException)
            }

            override def onSuccess(request: ChangeMessageVisibilityBatchRequest,
                                   result: ChangeMessageVisibilityBatchResult): Unit =
              if (!result.getFailed.isEmpty) {
                val nrOfFailedMessages = result.getFailed.size()
                val batchException: SqsBatchException =
                  new SqsBatchException(
                    batchSize = nrOfActions,
                    cause = new Exception(
                      s"Some messages failed to change visibility. $nrOfFailedMessages of $nrOfActions messages failed"
                    )
                  )
                responsePromise.failure(batchException)
                failureCallback.invoke(batchException)
              } else {
                responsePromise.success(actions.map(a => new SqsAckResult(Some(result), a)))
                changeVisibilityCallback.invoke(request)
              }
          }

        sqsClient.changeMessageVisibilityBatchAsync(createBatchRequest(actions), handler)
        push(out, responsePromise.future)
      }

      private def createBatchRequest(
          actions: Seq[MessageAction.ChangeMessageVisibility]
      ): ChangeMessageVisibilityBatchRequest = {
        val entries = actions.zipWithIndex.map {
          case (a, i) =>
            new ChangeMessageVisibilityBatchRequestEntry()
              .withId(i.toString)
              .withReceiptHandle(a.message.getReceiptHandle)
              .withVisibilityTimeout(a.visibilityTimeout)
        }
        new ChangeMessageVisibilityBatchRequest()
          .withQueueUrl(queueUrl)
          .withEntries(entries.asJava)
      }
    }
}
