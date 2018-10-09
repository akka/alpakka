/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.sqs.scaladsl

import akka.NotUsed
import akka.stream.alpakka.sqs.impl.{SqsBatchFlowStage, SqsFlowStage}
import akka.stream.alpakka.sqs.{
  SqsPublishBatchSettings,
  SqsPublishGroupedSettings,
  SqsPublishResult,
  SqsPublishSettings
}
import akka.stream.scaladsl.Flow
import com.amazonaws.services.sqs.AmazonSQSAsync
import com.amazonaws.services.sqs.model.SendMessageRequest

/**
 * Scala API to create publishing SQS flows.
 */
object SqsPublishFlow {

  /**
    * creates a [[akka.stream.scaladsl.Flow Flow]] to publish messages to a SQS queue using an [[com.amazonaws.services.sqs.AmazonSQSAsync AmazonSQSAsync]]
    */
  def apply(queueUrl: String, settings: SqsPublishSettings)(
      implicit sqsClient: AmazonSQSAsync
  ): Flow[SendMessageRequest, SqsPublishResult, NotUsed] =
    Flow
      .fromFunction((r: SendMessageRequest) => r.withQueueUrl(queueUrl))
      .via(new SqsFlowStage(sqsClient))
      .mapAsync(settings.maxInFlight)(identity)

  /**
    * creates a [[akka.stream.scaladsl.Flow Flow]] to publish messages to a SQS queue using an [[com.amazonaws.services.sqs.AmazonSQSAsync AmazonSQSAsync]]
    */
  def apply(queueUrl: String)(
      implicit sqsClient: AmazonSQSAsync
  ): Flow[SendMessageRequest, SqsPublishResult, NotUsed] = apply(queueUrl, SqsPublishSettings.Defaults)

  /**
    * creates a [[akka.stream.scaladsl.Flow Flow]] to publish messages to SQS queues based on the message queue url using an [[com.amazonaws.services.sqs.AmazonSQSAsync AmazonSQSAsync]]
    */
  def apply(settings: SqsPublishSettings = SqsPublishSettings.Defaults)(
      implicit sqsClient: AmazonSQSAsync
  ): Flow[SendMessageRequest, SqsPublishResult, NotUsed] =
    Flow
      .fromGraph(new SqsFlowStage(sqsClient))
      .mapAsync(settings.maxInFlight)(identity)


  /**
    * creates a [[akka.stream.scaladsl.Flow Flow]] that groups messages and publishes them in batches to a SQS queue using an [[com.amazonaws.services.sqs.AmazonSQSAsync AmazonSQSAsync]]
    * @see https://doc.akka.io/docs/akka/current/stream/operators/Source-or-Flow/groupedWithin.html#groupedwithin
    */
  def grouped(queueUrl: String, settings: SqsPublishGroupedSettings = SqsPublishGroupedSettings.Defaults)(
      implicit sqsClient: AmazonSQSAsync
  ): Flow[SendMessageRequest, SqsPublishResult, NotUsed] =
    Flow[SendMessageRequest]
      .groupedWithin(settings.maxBatchSize, settings.maxBatchWait)
      .via(new SqsBatchFlowStage(queueUrl, sqsClient))
      .mapAsync(settings.concurrentRequests)(identity)
      .mapConcat(identity)

  /**
    * creates a [[akka.stream.scaladsl.Flow Flow]] to publish messages in batches to a SQS queue using an [[com.amazonaws.services.sqs.AmazonSQSAsync AmazonSQSAsync]]
    */
  def batch(queueUrl: String, settings: SqsPublishBatchSettings = SqsPublishBatchSettings.Defaults)(
      implicit sqsClient: AmazonSQSAsync
  ): Flow[Iterable[SendMessageRequest], List[SqsPublishResult], NotUsed] =
    Flow
      .fromGraph(new SqsBatchFlowStage(queueUrl, sqsClient))
      .mapAsync(settings.concurrentRequests)(identity)
}
