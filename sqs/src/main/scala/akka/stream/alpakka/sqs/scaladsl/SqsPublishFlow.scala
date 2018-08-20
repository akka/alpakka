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
   * Create a flow publishing `SendMessageRequest` messages to a SQS queue.
   */
  def apply(queueUrl: String, settings: SqsPublishSettings = SqsPublishSettings.Defaults)(
      implicit sqsClient: AmazonSQSAsync
  ): Flow[SendMessageRequest, SqsPublishResult, NotUsed] =
    Flow
      .fromGraph(new SqsFlowStage(queueUrl, sqsClient))
      .mapAsync(settings.maxInFlight)(identity)

  /**
   * Create a flow grouping and publishing `SendMessageRequest` messages in batches to a SQS queue.
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
   * Create a flow publishing `SendMessageRequest` messages in batches to a SQS queue.
   */
  def batch(queueUrl: String, settings: SqsPublishBatchSettings = SqsPublishBatchSettings.Defaults)(
      implicit sqsClient: AmazonSQSAsync
  ): Flow[Iterable[SendMessageRequest], List[SqsPublishResult], NotUsed] =
    Flow
      .fromGraph(new SqsBatchFlowStage(queueUrl, sqsClient))
      .mapAsync(settings.concurrentRequests)(identity)
}
