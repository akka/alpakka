/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.sqs.scaladsl

import java.util.concurrent.CompletionException

import akka.NotUsed
import akka.annotation.{ApiMayChange, InternalApi}
import akka.dispatch.ExecutionContexts.sameThreadExecutionContext
import akka.stream.FlowShape
import akka.stream.alpakka.sqs.MessageAction._
import akka.stream.alpakka.sqs.SqsAckResult._
import akka.stream.alpakka.sqs.SqsAckResultEntry._
import akka.stream.alpakka.sqs._
import akka.stream.scaladsl.{Flow, GraphDSL, Merge, Partition}
import software.amazon.awssdk.services.sqs.SqsAsyncClient
import software.amazon.awssdk.services.sqs.model._

import scala.collection.JavaConverters._
import scala.collection.immutable
import scala.compat.java8.FutureConverters._
import scala.concurrent.Future

/**
 * Scala API to create acknowledging SQS flows.
 */
@ApiMayChange
object SqsAckFlow {

  /**
   * creates a [[akka.stream.scaladsl.Flow Flow]] for ack a single SQS message at a time using an [[software.amazon.awssdk.services.sqs.SqsAsyncClient]].
   */
  def apply(queueUrl: String, settings: SqsAckSettings = SqsAckSettings.Defaults)(implicit
      sqsClient: SqsAsyncClient
  ): Flow[MessageAction, SqsAckResult, NotUsed] = {
    checkClient(sqsClient)
    Flow[MessageAction]
      .mapAsync(settings.maxInFlight) {
        case messageAction: MessageAction.Delete =>
          val request =
            DeleteMessageRequest
              .builder()
              .queueUrl(queueUrl)
              .receiptHandle(messageAction.message.receiptHandle())
              .build()

          sqsClient
            .deleteMessage(request)
            .toScala
            .map(resp => new SqsDeleteResult(messageAction, resp))(sameThreadExecutionContext)

        case messageAction: MessageAction.ChangeMessageVisibility =>
          val request =
            ChangeMessageVisibilityRequest
              .builder()
              .queueUrl(queueUrl)
              .receiptHandle(messageAction.message.receiptHandle())
              .visibilityTimeout(messageAction.visibilityTimeout)
              .build()

          sqsClient
            .changeMessageVisibility(request)
            .toScala
            .map(resp => new SqsChangeMessageVisibilityResult(messageAction, resp))(sameThreadExecutionContext)

        case messageAction: MessageAction.Ignore =>
          Future.successful(new SqsIgnoreResult(messageAction))
      }
  }

  /**
   * creates a [[akka.stream.scaladsl.Flow Flow]] for ack grouped SQS messages using an [[software.amazon.awssdk.services.sqs.SqsAsyncClient]].
   */
  def grouped(queueUrl: String, settings: SqsAckGroupedSettings = SqsAckGroupedSettings.Defaults)(implicit
      sqsClient: SqsAsyncClient
  ): Flow[MessageAction, SqsAckResultEntry, NotUsed] = {
    checkClient(sqsClient)
    Flow.fromGraph(
      GraphDSL.create() { implicit builder =>
        import GraphDSL.Implicits._

        val p = builder.add(
          Partition[MessageAction](3,
                                   {
                                     case _: Delete => 0
                                     case _: ChangeMessageVisibility => 1
                                     case _: Ignore => 2
                                   }
          )
        )

        val merge = builder.add(Merge[SqsAckResultEntry](3))

        val mapDelete = Flow[MessageAction].collectType[Delete]
        val mapChangeMessageVisibility = Flow[MessageAction].collectType[ChangeMessageVisibility]
        val mapChangeIgnore = Flow[MessageAction].collectType[Ignore]

        p.out(0) ~> mapDelete ~> groupedDelete(queueUrl, settings) ~> merge
        p.out(1) ~> mapChangeMessageVisibility ~> groupedChangeMessageVisibility(queueUrl, settings) ~> merge
        p.out(2) ~> mapChangeIgnore ~> Flow[Ignore].map(new SqsIgnoreResultEntry(_)) ~> merge

        FlowShape(p.in, merge.out)
      }
    )
  }

  private def groupedDelete(queueUrl: String, settings: SqsAckGroupedSettings)(implicit
      sqsClient: SqsAsyncClient
  ): Flow[MessageAction.Delete, SqsDeleteResultEntry, NotUsed] = {
    checkClient(sqsClient)
    Flow[MessageAction.Delete]
      .groupedWithin(settings.maxBatchSize, settings.maxBatchWait)
      .map { actions =>
        val entries = actions.zipWithIndex.map { case (a, i) =>
          DeleteMessageBatchRequestEntry
            .builder()
            .id(i.toString)
            .receiptHandle(a.message.receiptHandle())
            .build()
        }

        actions -> DeleteMessageBatchRequest
          .builder()
          .queueUrl(queueUrl)
          .entries(entries.asJava)
          .build()
      }
      .mapAsync(settings.concurrentRequests) { case (actions: immutable.Seq[Delete], request) =>
        sqsClient
          .deleteMessageBatch(request)
          .toScala
          .map {
            case response if response.failed().isEmpty =>
              val responseMetadata = response.responseMetadata()
              val resultEntries = response.successful().asScala.map(e => e.id.toInt -> e).toMap
              actions.zipWithIndex.map { case (a, i) =>
                val result = resultEntries(i)
                new SqsDeleteResultEntry(a, result, responseMetadata)
              }
            case resp =>
              val numberOfMessages = request.entries().size()
              val nrOfFailedMessages = resp.failed().size()
              throw new SqsBatchException(
                numberOfMessages,
                s"Some messages are failed to delete. $nrOfFailedMessages of $numberOfMessages messages are failed"
              )
          }(sameThreadExecutionContext)
          .recoverWith {
            case e: CompletionException =>
              Future.failed(new SqsBatchException(request.entries().size(), e.getMessage, e.getCause))
            case e =>
              Future.failed(new SqsBatchException(request.entries().size(), e.getMessage, e))
          }(sameThreadExecutionContext)
      }
      .mapConcat(identity)
  }

  private def groupedChangeMessageVisibility(queueUrl: String, settings: SqsAckGroupedSettings)(implicit
      sqsClient: SqsAsyncClient
  ): Flow[MessageAction.ChangeMessageVisibility, SqsChangeMessageVisibilityResultEntry, NotUsed] =
    Flow[MessageAction.ChangeMessageVisibility]
      .groupedWithin(settings.maxBatchSize, settings.maxBatchWait)
      .map { actions =>
        val entries = actions.zipWithIndex.map { case (a, i) =>
          ChangeMessageVisibilityBatchRequestEntry
            .builder()
            .id(i.toString)
            .receiptHandle(a.message.receiptHandle())
            .visibilityTimeout(a.visibilityTimeout)
            .build()
        }

        actions -> ChangeMessageVisibilityBatchRequest
          .builder()
          .queueUrl(queueUrl)
          .entries(entries.asJava)
          .build()
      }
      .mapAsync(settings.concurrentRequests) { case (actions, request) =>
        sqsClient
          .changeMessageVisibilityBatch(request)
          .toScala
          .map {
            case response if response.failed().isEmpty =>
              val responseMetadata = response.responseMetadata()
              val resultEntries = response.successful().asScala.map(e => e.id.toInt -> e).toMap
              actions.zipWithIndex.map { case (a, i) =>
                val result = resultEntries(i)
                new SqsChangeMessageVisibilityResultEntry(a, result, responseMetadata)
              }
            case resp =>
              val numberOfMessages = request.entries().size()
              val nrOfFailedMessages = resp.failed().size()
              throw new SqsBatchException(
                numberOfMessages,
                s"Some messages are failed to change visibility. $nrOfFailedMessages of $numberOfMessages messages are failed"
              )
          }(sameThreadExecutionContext)
          .recoverWith {
            case e: CompletionException =>
              Future.failed(new SqsBatchException(request.entries().size(), e.getMessage, e.getCause))
            case e =>
              Future.failed(new SqsBatchException(request.entries().size(), e.getMessage, e))
          }(sameThreadExecutionContext)
      }
      .mapConcat(identity)

  @InternalApi
  private[scaladsl] def checkClient(sqsClient: SqsAsyncClient): Unit =
    require(sqsClient != null, "The `SqsAsyncClient` passed in may not be null.")
}
