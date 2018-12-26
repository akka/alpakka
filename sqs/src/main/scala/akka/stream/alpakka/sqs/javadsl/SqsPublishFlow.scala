/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.sqs.javadsl

import akka.NotUsed
import akka.stream.alpakka.sqs._
import akka.stream.javadsl.Flow
import akka.stream.scaladsl.{Flow => SFlow}
import com.amazonaws.services.sqs.AmazonSQSAsync

import com.amazonaws.services.sqs.model.SendMessageRequest

import scala.collection.JavaConverters._

/**
 * Java API to create SQS flows.
 */
object SqsPublishFlow {

  /**
   * creates a [[akka.stream.javadsl.Flow Flow]] to publish messages to a SQS queue using an [[com.amazonaws.services.sqs.AmazonSQSAsync AmazonSQSAsync]]
   */
  def create(queueUrl: String,
             settings: SqsPublishSettings,
             sqsClient: AmazonSQSAsync): Flow[SendMessageRequest, SqsPublishResult, NotUsed] =
    scaladsl.SqsPublishFlow.apply(queueUrl, settings)(sqsClient).asJava

  /**
   * creates a [[akka.stream.javadsl.Flow Flow]] to publish messages to SQS queues based on the message queue url using an [[com.amazonaws.services.sqs.AmazonSQSAsync AmazonSQSAsync]]
   */
  def create(settings: SqsPublishSettings,
             sqsClient: AmazonSQSAsync): Flow[SendMessageRequest, SqsPublishResult, NotUsed] =
    scaladsl.SqsPublishFlow.apply(settings)(sqsClient).asJava

  /**
   * creates a [[akka.stream.javadsl.Flow Flow]] that groups messages and publish them in batches to a SQS queue using an [[com.amazonaws.services.sqs.AmazonSQSAsync AmazonSQSAsync]]
   * @see https://doc.akka.io/docs/akka/current/stream/operators/Source-or-Flow/groupedWithin.html#groupedwithin
   */
  def grouped(queueUrl: String,
              settings: SqsPublishGroupedSettings,
              sqsClient: AmazonSQSAsync): Flow[SendMessageRequest, SqsPublishResult, NotUsed] =
    scaladsl.SqsPublishFlow.grouped(queueUrl, settings)(sqsClient).asJava

  /**
   * creates a [[akka.stream.javadsl.Flow Flow]] to publish messages in batches to a SQS queue using an [[com.amazonaws.services.sqs.AmazonSQSAsync AmazonSQSAsync]]
   */
  def batch(
      queueUrl: String,
      settings: SqsPublishBatchSettings,
      sqsClient: AmazonSQSAsync
  ): Flow[java.lang.Iterable[SendMessageRequest], java.util.List[SqsPublishResult], NotUsed] =
    SFlow[java.lang.Iterable[SendMessageRequest]]
      .map(_.asScala)
      .via(scaladsl.SqsPublishFlow.batch(queueUrl, settings)(sqsClient))
      .map(_.asJava)
      .asJava

}
