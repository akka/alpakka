/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.sqs.scaladsl

import akka.Done
import akka.stream.alpakka.sqs.{SqsPublishBatchSettings, SqsPublishGroupedSettings, SqsPublishSettings}
import akka.stream.scaladsl.{Flow, Keep, Sink}
import com.amazonaws.services.sqs.AmazonSQSAsync
import com.amazonaws.services.sqs.model.SendMessageRequest

import scala.concurrent.Future

/**
 * Scala API to create SQS Sinks.
 */
object SqsPublishSink {

  /**
   * Creates a sink for a SQS queue accepting Strings.
   */
  def apply(queueUrl: String, settings: SqsPublishSettings = SqsPublishSettings.Defaults)(
      implicit sqsClient: AmazonSQSAsync
  ): Sink[String, Future[Done]] =
    Flow
      .fromFunction((msg: String) => new SendMessageRequest(queueUrl, msg))
      .toMat(messageSink(queueUrl, settings))(Keep.right)

  /**
   * Create a grouped sink running in batch mode for a SQS queue accepting Strings.
   */
  def grouped(queueUrl: String, settings: SqsPublishGroupedSettings = SqsPublishGroupedSettings.Defaults)(
      implicit sqsClient: AmazonSQSAsync
  ): Sink[String, Future[Done]] =
    Flow
      .fromFunction((msg: String) => new SendMessageRequest(queueUrl, msg))
      .toMat(groupedMessageSink(queueUrl, settings))(Keep.right)

  /**
   * Create a sink running in batch mode for a SQS queue accepting collections of Strings.
   */
  def batch(queueUrl: String, settings: SqsPublishBatchSettings = SqsPublishBatchSettings.Defaults)(
      implicit sqsClient: AmazonSQSAsync
  ): Sink[Iterable[String], Future[Done]] =
    Flow
      .fromFunction((msgs: Iterable[String]) => msgs.map(msg => new SendMessageRequest(queueUrl, msg)))
      .toMat(batchedMessageSink(queueUrl, settings))(Keep.right)

  /**
   * Creates a sink for a SQS queue accepting SendMessageRequests.
   */
  def messageSink(queueUrl: String, settings: SqsPublishSettings = SqsPublishSettings.Defaults)(
      implicit sqsClient: AmazonSQSAsync
  ): Sink[SendMessageRequest, Future[Done]] =
    SqsPublishFlow.apply(queueUrl, settings).toMat(Sink.ignore)(Keep.right)

  /**
   * Create a flow grouping and publishing `SendMessageRequest` messages in batches to a SQS queue.
   * @see https://doc.akka.io/docs/akka/current/stream/operators/Source-or-Flow/groupedWithin.html#groupedwithin
   */
  def groupedMessageSink(queueUrl: String, settings: SqsPublishGroupedSettings = SqsPublishGroupedSettings.Defaults)(
      implicit sqsClient: AmazonSQSAsync
  ): Sink[SendMessageRequest, Future[Done]] =
    SqsPublishFlow.grouped(queueUrl, settings).toMat(Sink.ignore)(Keep.right)

  /**
   * Create a flow publishing `SendMessageRequest` messages in batches to a SQS queue.
   */
  def batchedMessageSink(queueUrl: String, settings: SqsPublishBatchSettings = SqsPublishBatchSettings.Defaults)(
      implicit sqsClient: AmazonSQSAsync
  ): Sink[Iterable[SendMessageRequest], Future[Done]] =
    SqsPublishFlow.batch(queueUrl, settings).toMat(Sink.ignore)(Keep.right)
}
