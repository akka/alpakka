/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.sqs.scaladsl

import akka.NotUsed
import akka.annotation.ApiMayChange
import akka.dispatch.ExecutionContexts.sameThreadExecutionContext
import akka.stream.alpakka.sqs._
import akka.stream.scaladsl.Flow
import software.amazon.awssdk.services.sqs.SqsAsyncClient
import software.amazon.awssdk.services.sqs.model._

import scala.collection.JavaConverters._
import scala.compat.java8.FutureConverters._
import scala.util.Try
import scala.util.{Failure, Success}

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
  ): Flow[SendMessageRequest, SqsPublishResult, NotUsed] =
    Flow[SendMessageRequest]
      .mapAsync(settings.maxInFlight) { req =>
        sqsClient
          .sendMessage(req)
          .toScala
          .map(req -> _)(sameThreadExecutionContext)
      }
      .map { case (request, response) => new SqsPublishResult(request, response) }

  /**
   * creates a [[akka.stream.scaladsl.Flow Flow]] that groups messages and publishes them in batches to a SQS queue using an [[software.amazon.awssdk.services.sqs.SqsAsyncClient SqsAsyncClient]]
   *
   * @see https://doc.akka.io/docs/akka/current/stream/operators/Source-or-Flow/groupedWithin.html#groupedwithin
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
  ): Flow[Iterable[SendMessageRequest], List[SqsPublishResultEntry], NotUsed] =
    Flow[Iterable[SendMessageRequest]]
      .mapAsync(settings.concurrentRequests) { requests =>
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

        val request = SendMessageBatchRequest
          .builder()
          .queueUrl(queueUrl)
          .entries(entries.toList.asJava)
          .build()

        sqsClient
          .sendMessageBatch(request)
          .toScala
          .map(response => requests -> response)(sameThreadExecutionContext)
      }
      .mapConcat {
        case (requests, response) =>
          val responseMetadata = response.responseMetadata()
          val idToRequest = requests.zipWithIndex.map(_.swap).toMap
          val successful = response
            .successful()
            .asScala
            .map { e =>
              Success(new SqsPublishResultEntry(idToRequest(e.id.toInt), e, responseMetadata))
            }
            .toList
          val failed = response
            .failed()
            .asScala
            .map { e =>
              Failure(new SqsBatchException(requests.size, e.message()))
            }.toList
          Stream(successful, failed).map(_.map(_.get))
      }
}
