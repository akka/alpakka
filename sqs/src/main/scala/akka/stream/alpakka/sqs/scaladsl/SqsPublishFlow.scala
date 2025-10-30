/*
 * Copyright (C) since 2016 Lightbend Inc. <https://akka.io>
 */

package akka.stream.alpakka.sqs.scaladsl

import java.util.concurrent.CompletionException

import akka.NotUsed
import akka.annotation.ApiMayChange
import akka.stream.alpakka.sqs.{SqsBatchException, _}
import akka.stream.scaladsl.{Flow, Source}
import software.amazon.awssdk.services.sqs.SqsAsyncClient
import software.amazon.awssdk.services.sqs.model._

import scala.jdk.CollectionConverters._
import scala.concurrent.ExecutionContext
import scala.jdk.FutureConverters._

/**
 * Scala API to create publishing SQS flows.
 */
@ApiMayChange
object SqsPublishFlow {

  /**
   * creates a [[akka.stream.scaladsl.Flow Flow]] to publish messages to a SQS queue using an [[software.amazon.awssdk.services.sqs.SqsAsyncClient SqsAsyncClient]]
   */
  def apply(queueUrl: String, settings: SqsPublishSettings)(
      implicit sqsClient: SqsAsyncClient
  ): Flow[SendMessageRequest, SqsPublishResult, NotUsed] =
    Flow
      .fromFunction((r: SendMessageRequest) => r.toBuilder.queueUrl(queueUrl).build())
      .via(apply(settings))

  /**
   * creates a [[akka.stream.scaladsl.Flow Flow]] to publish messages to a SQS queue using an [[software.amazon.awssdk.services.sqs.SqsAsyncClient SqsAsyncClient]]
   */
  def apply(queueUrl: String)(
      implicit sqsClient: SqsAsyncClient
  ): Flow[SendMessageRequest, SqsPublishResult, NotUsed] =
    apply(queueUrl, SqsPublishSettings.Defaults)

  /**
   * creates a [[akka.stream.scaladsl.Flow Flow]] to publish messages to SQS queues based on the message queue url using an [[software.amazon.awssdk.services.sqs.SqsAsyncClient SqsAsyncClient]]
   */
  def apply(settings: SqsPublishSettings = SqsPublishSettings.Defaults)(
      implicit sqsClient: SqsAsyncClient
  ): Flow[SendMessageRequest, SqsPublishResult, NotUsed] = {
    SqsAckFlow.checkClient(sqsClient)
    Flow[SendMessageRequest]
      .mapAsync(settings.maxInFlight) { req =>
        sqsClient
          .sendMessage(req)
          .asScala
          .map(req -> _)(ExecutionContext.parasitic)
      }
      .map { case (request, response) => new SqsPublishResult(request, response) }
  }

  /**
   * creates a [[akka.stream.scaladsl.Flow Flow]] that groups messages and publishes them in batches to a SQS queue using an [[software.amazon.awssdk.services.sqs.SqsAsyncClient SqsAsyncClient]]
   *
   * @see https://doc.akka.io/libraries/akka-core/current/stream/operators/Source-or-Flow/groupedWithin.html#groupedwithin
   */
  def grouped(queueUrl: String, settings: SqsPublishGroupedSettings = SqsPublishGroupedSettings.Defaults)(
      implicit sqsClient: SqsAsyncClient
  ): Flow[SendMessageRequest, SqsPublishResultEntry, NotUsed] =
    Flow[SendMessageRequest]
      .groupedWithin(settings.maxBatchSize, settings.maxBatchWait)
      .via(batch(queueUrl, SqsPublishBatchSettings.create().withConcurrentRequests(settings.concurrentRequests)))
      .mapConcat(identity)

  /**
   * creates a [[akka.stream.scaladsl.Flow Flow]] to publish messages in batches to a SQS queue using an [[software.amazon.awssdk.services.sqs.SqsAsyncClient SqsAsyncClient]]
   */
  def batch(queueUrl: String, settings: SqsPublishBatchSettings = SqsPublishBatchSettings.Defaults)(
      implicit sqsClient: SqsAsyncClient
  ): Flow[Iterable[SendMessageRequest], List[SqsPublishResultEntry], NotUsed] = {
    SqsAckFlow.checkClient(sqsClient)
    Flow[Iterable[SendMessageRequest]]
      .map { requests =>
        val entries = requests.zipWithIndex.map {
          case (r, i) =>
            SendMessageBatchRequestEntry
              .builder()
              .id(i.toString)
              .messageBody(r.messageBody())
              .messageAttributes(r.messageAttributes())
              .messageGroupId(r.messageGroupId())
              .messageDeduplicationId(r.messageDeduplicationId())
              .delaySeconds(r.delaySeconds())
              .build()
        }

        requests -> SendMessageBatchRequest
          .builder()
          .queueUrl(queueUrl)
          .entries(entries.toList.asJava)
          .build()
      }
      .mapAsync(settings.concurrentRequests) {
        case (requests, batchRequest) =>
          sqsClient
            .sendMessageBatch(batchRequest)
            .asScala
            .map {
              case response if response.failed().isEmpty =>
                val responseMetadata = response.responseMetadata()
                val resultEntries = response.successful().asScala.map(e => e.id.toInt -> e).toMap
                requests.zipWithIndex.map {
                  case (r, i) =>
                    val result = resultEntries(i)
                    new SqsPublishResultEntry(r, result, responseMetadata)
                }.toList
              case response =>
                val numberOfMessages = batchRequest.entries().size()
                val nrOfFailedMessages = response.failed().size()
                throw new SqsBatchException(
                  numberOfMessages,
                  s"Some messages are failed to send. $nrOfFailedMessages of $numberOfMessages messages are failed"
                )
            }(ExecutionContext.parasitic)
      }
      .recoverWithRetries(1, {
        case e: CompletionException =>
          Source.failed(e.getCause)
        case e: SqsBatchException =>
          Source.failed(e)
        case e =>
          Source.failed(e)
      })
  }
}
