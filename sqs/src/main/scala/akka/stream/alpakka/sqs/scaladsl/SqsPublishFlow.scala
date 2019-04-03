/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.sqs.scaladsl

import java.util.concurrent.CompletionException

import akka.NotUsed
import akka.annotation.ApiMayChange
import akka.dispatch.ExecutionContexts.sameThreadExecutionContext
import akka.stream.alpakka.sqs.{SqsBatchException, SqsPublishResult, _}
import akka.stream.scaladsl.{Flow, Source}
import software.amazon.awssdk.services.sqs.SqsAsyncClient
import software.amazon.awssdk.services.sqs.model._

import scala.collection.JavaConverters._
import scala.compat.java8.FutureConverters._
import scala.concurrent.Future

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
      .map {
        case (request, response) =>
          val fifoIdentifiers = Option(response.sequenceNumber()).map { sequenceNumber =>
            val messageGroupId = request.messageGroupId()
            val messageDeduplicationId = Option(request.messageDeduplicationId())
            new FifoMessageIdentifiers(sequenceNumber, messageGroupId, messageDeduplicationId)
          }

          new SqsPublishResult(response, fifoIdentifiers)
      }

  /**
   * creates a [[akka.stream.scaladsl.Flow Flow]] that groups messages and publishes them in batches to a SQS queue using an [[software.amazon.awssdk.services.sqs.SqsAsyncClient SqsAsyncClient]]
   *
   * @see https://doc.akka.io/docs/akka/current/stream/operators/Source-or-Flow/groupedWithin.html#groupedwithin
   */
  def grouped(queueUrl: String, settings: SqsPublishGroupedSettings = SqsPublishGroupedSettings.Defaults)(
      implicit sqsClient: SqsAsyncClient
  ): Flow[SendMessageRequest, SqsPublishResult[SendMessageBatchResponse], NotUsed] =
    Flow[SendMessageRequest]
      .groupedWithin(settings.maxBatchSize, settings.maxBatchWait)
      .via(batch(queueUrl, SqsPublishBatchSettings.create().withConcurrentRequests(settings.concurrentRequests)))
      .mapConcat(identity)

  /**
   * creates a [[akka.stream.scaladsl.Flow Flow]] to publish messages in batches to a SQS queue using an [[software.amazon.awssdk.services.sqs.SqsAsyncClient SqsAsyncClient]]
   */
  def batch(queueUrl: String, settings: SqsPublishBatchSettings = SqsPublishBatchSettings.Defaults)(
      implicit sqsClient: SqsAsyncClient
  ): Flow[Iterable[SendMessageRequest], List[SqsPublishResult[SendMessageBatchResponse]], NotUsed] =
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

        SendMessageBatchRequest
          .builder()
          .queueUrl(queueUrl)
          .entries(entries.toList.asJava)
          .build()
      }
      .mapAsync(settings.concurrentRequests) { req =>
        sqsClient
          .sendMessageBatch(req)
          .toScala
          .map(req -> _)(sameThreadExecutionContext)
      }
      .mapAsync(settings.concurrentRequests) {
        case (request, response) if !response.failed().isEmpty =>
          val nrOfFailedMessages = response.failed().size()
          val numberOfMessages = request.entries().size()

          Future.failed(
            new SqsBatchException(
              numberOfMessages,
              s"Some messages are failed to send. $nrOfFailedMessages of $numberOfMessages messages are failed"
            )
          )

        case (request, response) =>
          val requestEntries = request.entries().asScala.map(e => e.id -> e).toMap

          def result =
            response
              .successful()
              .asScala
              .map { resp =>
                val req = requestEntries(resp.id)

                val fifoIdentifiers = Option(resp.sequenceNumber()).map { sequenceNumber =>
                  val messageGroupId = req.messageGroupId()
                  val messageDeduplicationId = Option(req.messageDeduplicationId())
                  new FifoMessageIdentifiers(sequenceNumber, messageGroupId, messageDeduplicationId)
                }

                new SqsPublishResult(response, fifoIdentifiers)
              }

          Future.successful(result.toList)
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
