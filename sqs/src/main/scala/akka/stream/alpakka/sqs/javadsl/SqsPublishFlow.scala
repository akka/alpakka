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
   * Create a flow publishing `SendMessageRequest` messages to an SQS queue.
   */
  def create(queueUrl: String,
             settings: SqsPublishSettings,
             sqsClient: AmazonSQSAsync): Flow[SendMessageRequest, SqsPublishResult, NotUsed] =
    scaladsl.SqsPublishFlow.apply(queueUrl, settings)(sqsClient).asJava

  /**
   * Create a flow grouping and publishing `SendMessageRequest` messages to an SQS queue.
   *
   * @see https://doc.akka.io/docs/akka/current/stream/operators/Source-or-Flow/groupedWithin.html#groupedwithin
   */
  def grouped(queueUrl: String,
              settings: SqsPublishGroupedSettings,
              sqsClient: AmazonSQSAsync): Flow[SendMessageRequest, SqsPublishResult, NotUsed] =
    scaladsl.SqsPublishFlow.grouped(queueUrl, settings)(sqsClient).asJava

  /**
   * Create a flow publishing batches of `SendMessageRequest` messages to an SQS queue.
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
