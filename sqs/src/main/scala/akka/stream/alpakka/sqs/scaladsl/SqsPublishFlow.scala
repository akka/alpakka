/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.sqs.scaladsl

import java.util.concurrent.CompletionException

import akka.NotUsed
import akka.dispatch.ExecutionContexts.sameThreadExecutionContext
import akka.stream.alpakka.sqs.{SqsBatchException, SqsPublishResult, _}
import akka.stream.scaladsl.{Flow, Source}
import software.amazon.awssdk.services.sqs.SqsAsyncClient
import software.amazon.awssdk.services.sqs.model._

import scala.collection.JavaConverters._
import scala.compat.java8.FutureConverters._

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
  ): Flow[SendMessageRequest, SqsPublishResult[SendMessageResponse], NotUsed] =
    Flow
      .fromFunction((r: SendMessageRequest) => r.toBuilder.queueUrl(queueUrl).build())
      .via(apply(settings))

  /**
   * creates a [[akka.stream.scaladsl.Flow Flow]] to publish messages to a SQS queue using an [[software.amazon.awssdk.services.sqs.SqsAsyncClient SqsAsyncClient]]
   */
  def apply(queueUrl: String)(
      implicit sqsClient: SqsAsyncClient
  ): Flow[SendMessageRequest, SqsPublishResult[SendMessageResponse], NotUsed] =
    apply(queueUrl, SqsPublishSettings.Defaults)

  /**
   * creates a [[akka.stream.scaladsl.Flow Flow]] to publish messages to SQS queues based on the message queue url using an [[software.amazon.awssdk.services.sqs.SqsAsyncClient SqsAsyncClient]]
   */
  def apply(settings: SqsPublishSettings = SqsPublishSettings.Defaults)(
      implicit sqsClient: SqsAsyncClient
  ): Flow[SendMessageRequest, SqsPublishResult[SendMessageResponse], NotUsed] =
    Flow[SendMessageRequest]
      .mapAsync(settings.maxInFlight) { req =>
        sqsClient
          .sendMessage(req)
          .toScala
          .map(req -> _)(sameThreadExecutionContext)
      }
      .map { case (request, response) => new SqsPublishResult(response.responseMetadata(), response, request) }

  /**
   * creates a [[akka.stream.scaladsl.Flow Flow]] that groups messages and publishes them in batches to a SQS queue using an [[software.amazon.awssdk.services.sqs.SqsAsyncClient SqsAsyncClient]]
   *
   * @see https://doc.akka.io/docs/akka/current/stream/operators/Source-or-Flow/groupedWithin.html#groupedwithin
   */
  def grouped(queueUrl: String, settings: SqsPublishGroupedSettings = SqsPublishGroupedSettings.Defaults)(
      implicit sqsClient: SqsAsyncClient
  ): Flow[SendMessageRequest, SqsPublishResult[SendMessageBatchResultEntry], NotUsed] =
    Flow[SendMessageRequest]
      .groupedWithin(settings.maxBatchSize, settings.maxBatchWait)
      .via(batch(queueUrl, SqsPublishBatchSettings.create().withConcurrentRequests(settings.concurrentRequests)))
      .mapConcat(identity)

  /**
   * creates a [[akka.stream.scaladsl.Flow Flow]] to publish messages in batches to a SQS queue using an [[software.amazon.awssdk.services.sqs.SqsAsyncClient SqsAsyncClient]]
   */
  def batch(queueUrl: String, settings: SqsPublishBatchSettings = SqsPublishBatchSettings.Defaults)(
      implicit sqsClient: SqsAsyncClient
  ): Flow[Iterable[SendMessageRequest], List[SqsPublishResult[SendMessageBatchResultEntry]], NotUsed] =
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
            .toScala
            .map {
              case response if response.failed().isEmpty =>
                val responseMetadata = response.responseMetadata()
                val metadataEntries = response.successful().asScala.map(e => e.id.toInt -> e).toMap
                requests.zipWithIndex.map {
                  case (r, i) =>
                    val metadata = metadataEntries(i)
                    new SqsPublishResult(responseMetadata, metadata, r)
                }.toList
              case response =>
                val numberOfMessages = batchRequest.entries().size()
                val nrOfFailedMessages = response.failed().size()
                throw new SqsBatchException(
                  numberOfMessages,
                  s"Some messages are failed to send. $nrOfFailedMessages of $numberOfMessages messages are failed"
                )
            }(sameThreadExecutionContext)
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
