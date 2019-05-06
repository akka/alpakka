/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.sqs.javadsl

import akka.NotUsed
import akka.annotation.ApiMayChange
import akka.stream.alpakka.sqs.{
  SqsPublishBatchSettings,
  SqsPublishGroupedSettings,
  SqsPublishResult,
  SqsPublishSettings
}
import akka.stream.javadsl.Flow
import akka.stream.scaladsl.{Flow => SFlow}
import software.amazon.awssdk.services.sqs.SqsAsyncClient
import software.amazon.awssdk.services.sqs.model.{SendMessageBatchResponse, SendMessageRequest, SendMessageResponse}

import scala.collection.JavaConverters._

/**
 * Java API to create SQS flows.
 */
@ApiMayChange
object SqsPublishFlow {

  /**
   * creates a [[akka.stream.javadsl.Flow Flow]] to publish messages to a SQS queue using an [[software.amazon.awssdk.services.sqs.SqsAsyncClient AmazonSQSAsync]]
   */
  def create(queueUrl: String,
             settings: SqsPublishSettings,
             sqsClient: SqsAsyncClient): Flow[SendMessageRequest, SqsPublishResult[SendMessageResponse], NotUsed] =
    akka.stream.alpakka.sqs.scaladsl.SqsPublishFlow.apply(queueUrl, settings)(sqsClient).asJava

  /**
   * creates a [[akka.stream.javadsl.Flow Flow]] to publish messages to SQS queues based on the message queue url using an [[software.amazon.awssdk.services.sqs.SqsAsyncClient AmazonSQSAsync]]
   */
  def create(settings: SqsPublishSettings,
             sqsClient: SqsAsyncClient): Flow[SendMessageRequest, SqsPublishResult[SendMessageResponse], NotUsed] =
    akka.stream.alpakka.sqs.scaladsl.SqsPublishFlow.apply(settings)(sqsClient).asJava

  /**
   * creates a [[akka.stream.javadsl.Flow Flow]] that groups messages and publish them in batches to a SQS queue using an [[software.amazon.awssdk.services.sqs.SqsAsyncClient AmazonSQSAsync]]
   * @see https://doc.akka.io/docs/akka/current/stream/operators/Source-or-Flow/groupedWithin.html#groupedwithin
   */
  def grouped(
      queueUrl: String,
      settings: SqsPublishGroupedSettings,
      sqsClient: SqsAsyncClient
  ): Flow[SendMessageRequest, SqsPublishResult[SendMessageBatchResponse], NotUsed] =
    akka.stream.alpakka.sqs.scaladsl.SqsPublishFlow
      .grouped(queueUrl, settings)(sqsClient)
      .asJava

  /**
   * creates a [[akka.stream.javadsl.Flow Flow]] to publish messages in batches to a SQS queue using an [[software.amazon.awssdk.services.sqs.SqsAsyncClient AmazonSQSAsync]]
   */
  def batch(
      queueUrl: String,
      settings: SqsPublishBatchSettings,
      sqsClient: SqsAsyncClient
  ): Flow[java.lang.Iterable[SendMessageRequest], java.util.List[SqsPublishResult[SendMessageBatchResponse]], NotUsed] =
    SFlow[java.lang.Iterable[SendMessageRequest]]
      .map(_.asScala)
      .via(akka.stream.alpakka.sqs.scaladsl.SqsPublishFlow.batch(queueUrl, settings)(sqsClient))
      .map(_.asJava)
      .asJava
}
