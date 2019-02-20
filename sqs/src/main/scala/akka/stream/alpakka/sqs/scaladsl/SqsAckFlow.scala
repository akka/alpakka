/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.sqs.scaladsl

import java.util.concurrent.CompletionException

import akka.NotUsed
import akka.dispatch.ExecutionContexts.sameThreadExecutionContext
import akka.stream.FlowShape
import akka.stream.alpakka.sqs.MessageAction.{ChangeMessageVisibility, Delete}
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
object SqsAckFlow {

  /**
   * creates a [[akka.stream.scaladsl.Flow Flow]] for ack a single SQS message at a time using an [[software.amazon.awssdk.services.sqs.SqsAsyncClient]].
   */
  def apply(queueUrl: String, settings: SqsAckSettings = SqsAckSettings.Defaults)(
      implicit sqsClient: SqsAsyncClient
  ): Flow[MessageAction, SqsAckResult[SqsResponse], NotUsed] =
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
            .map(resp => new SqsAckResult(Some(resp: SqsResponse), messageAction))(sameThreadExecutionContext)

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
            .map(resp => new SqsAckResult(Some(resp: SqsResponse), messageAction))(sameThreadExecutionContext)

        case messageAction: MessageAction.Ignore =>
          Future.successful(new SqsAckResult[SqsResponse](None, messageAction))
      }

  /**
   * creates a [[akka.stream.scaladsl.Flow Flow]] for ack grouped SQS messages using an [[software.amazon.awssdk.services.sqs.SqsAsyncClient]].
   */
  def grouped(queueUrl: String, settings: SqsAckGroupedSettings = SqsAckGroupedSettings.Defaults)(
      implicit sqsClient: SqsAsyncClient
  ): Flow[MessageAction, SqsAckResult[SqsResponse], NotUsed] =
    Flow.fromGraph(
      GraphDSL.create() { implicit builder =>
        import GraphDSL.Implicits._

        val p = builder.add(Partition[MessageAction](3, {
          case _: Delete => 0
          case _: ChangeMessageVisibility => 1
          case _: MessageAction.Ignore => 2
        }))

        val merge = builder.add(Merge[SqsAckResult[SqsResponse]](3))

        val mapDelete = Flow[MessageAction].collectType[Delete]
        val mapChangeMessageVisibility = Flow[MessageAction].collectType[ChangeMessageVisibility]

        p.out(0) ~> mapDelete ~> groupedDelete(queueUrl, settings) ~> merge
        p.out(1) ~> mapChangeMessageVisibility ~> groupedChangeMessageVisibility(queueUrl, settings) ~> merge
        p.out(2) ~> Flow[MessageAction].map(x => new SqsAckResult[SqsResponse](None, x)) ~> merge

        FlowShape(p.in, merge.out)
      }
    )

  private def groupedDelete(queueUrl: String, settings: SqsAckGroupedSettings)(
      implicit sqsClient: SqsAsyncClient
  ): Flow[MessageAction.Delete, SqsAckResult[SqsResponse], NotUsed] =
    Flow[MessageAction.Delete]
      .groupedWithin(settings.maxBatchSize, settings.maxBatchWait)
      .map { actions =>
        val entries = actions.zipWithIndex.map {
          case (a, i) =>
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
      .mapAsync(settings.concurrentRequests) {
        case (actions: immutable.Seq[Delete], request) =>
          sqsClient
            .deleteMessageBatch(request)
            .toScala
            .flatMap {
              case resp if resp.failed().isEmpty =>
                Future.successful(actions.map(a => new SqsAckResult(Some(resp: SqsResponse), a)).toList)
              case resp =>
                val numberOfMessages = request.entries().size()
                val nrOfFailedMessages = resp.failed().size()

                Future.failed(
                  new SqsBatchException(
                    numberOfMessages,
                    s"Some messages are failed to delete. $nrOfFailedMessages of $numberOfMessages messages are failed"
                  )
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

  private def groupedChangeMessageVisibility(queueUrl: String, settings: SqsAckGroupedSettings)(
      implicit sqsClient: SqsAsyncClient
  ): Flow[MessageAction.ChangeMessageVisibility, SqsAckResult[SqsResponse], NotUsed] =
    Flow[MessageAction.ChangeMessageVisibility]
      .groupedWithin(settings.maxBatchSize, settings.maxBatchWait)
      .map { actions =>
        val entries = actions.zipWithIndex.map {
          case (a, i) =>
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
      .mapAsync(settings.concurrentRequests) {
        case (actions, request) =>
          sqsClient
            .changeMessageVisibilityBatch(request)
            .toScala
            .flatMap {
              case resp if resp.failed().isEmpty =>
                Future.successful(actions.map(a => new SqsAckResult(Some(resp: SqsResponse), a)).toList)
              case resp =>
                val numberOfMessages = request.entries().size()
                val nrOfFailedMessages = resp.failed().size()

                Future.failed(
                  new SqsBatchException(
                    numberOfMessages,
                    s"Some messages are failed to delete. $nrOfFailedMessages of $numberOfMessages messages are failed"
                  )
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
