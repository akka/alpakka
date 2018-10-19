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
 * Scala API to create publishing SQS sinks.
 */
object SqsPublishSink {

  /**
    * creates a [[akka.stream.scaladsl.Sink Sink]] that accepts strings and publishes them as messages to a SQS queue using an [[com.amazonaws.services.sqs.AmazonSQSAsync AmazonSQSAsync]]
    */
  def apply(queueUrl: String, settings: SqsPublishSettings = SqsPublishSettings.Defaults)(
      implicit sqsClient: AmazonSQSAsync
  ): Sink[String, Future[Done]] =
    Flow
      .fromFunction((msg: String) => new SendMessageRequest(queueUrl, msg))
      .toMat(messageSink(queueUrl, settings))(Keep.right)

  /**
    * creates a [[akka.stream.scaladsl.Sink Sink]] that groups strings and publishes them as messages in batches to a SQS queue using an [[com.amazonaws.services.sqs.AmazonSQSAsync AmazonSQSAsync]]
    * @see https://doc.akka.io/docs/akka/current/stream/operators/Source-or-Flow/groupedWithin.html#groupedwithin
    */
  def grouped(queueUrl: String, settings: SqsPublishGroupedSettings = SqsPublishGroupedSettings.Defaults)(
      implicit sqsClient: AmazonSQSAsync
  ): Sink[String, Future[Done]] =
    Flow
      .fromFunction((msg: String) => new SendMessageRequest(queueUrl, msg))
      .toMat(groupedMessageSink(queueUrl, settings))(Keep.right)

  /**
    * creates a [[akka.stream.scaladsl.Sink Sink]] that accepts an iterable of strings and publish them as messages in batches to a SQS queue using an [[com.amazonaws.services.sqs.AmazonSQSAsync AmazonSQSAsync]]
    * @see https://doc.akka.io/docs/akka/current/stream/operators/Source-or-Flow/groupedWithin.html#groupedwithin
    */
  def batch(queueUrl: String, settings: SqsPublishBatchSettings = SqsPublishBatchSettings.Defaults)(
      implicit sqsClient: AmazonSQSAsync
  ): Sink[Iterable[String], Future[Done]] =
    Flow
      .fromFunction((msgs: Iterable[String]) => msgs.map(msg => new SendMessageRequest(queueUrl, msg)))
      .toMat(batchedMessageSink(queueUrl, settings))(Keep.right)

  /**
    * creates a [[akka.stream.scaladsl.Sink Sink]] to publish messages to a SQS queue using an [[com.amazonaws.services.sqs.AmazonSQSAsync AmazonSQSAsync]]
   */
  def messageSink(queueUrl: String, settings: SqsPublishSettings)(
      implicit sqsClient: AmazonSQSAsync
  ): Sink[SendMessageRequest, Future[Done]] =
    SqsPublishFlow.apply(queueUrl, settings).toMat(Sink.ignore)(Keep.right)

  /**
    * creates a [[akka.stream.scaladsl.Sink Sink]] to publish messages to a SQS queue using an [[com.amazonaws.services.sqs.AmazonSQSAsync AmazonSQSAsync]]
   */
  def messageSink(queueUrl: String)(
      implicit sqsClient: AmazonSQSAsync
  ): Sink[SendMessageRequest, Future[Done]] =
    SqsPublishFlow.apply(queueUrl, SqsPublishSettings.Defaults).toMat(Sink.ignore)(Keep.right)

  /**
    * creates a [[akka.stream.scaladsl.Sink Sink]] to publish messages to SQS queues based on the message queue url using an [[com.amazonaws.services.sqs.AmazonSQSAsync AmazonSQSAsync]]
   */
  def messageSink(settings: SqsPublishSettings = SqsPublishSettings.Defaults)(
      implicit sqsClient: AmazonSQSAsync
  ): Sink[SendMessageRequest, Future[Done]] =
    SqsPublishFlow.apply(settings).toMat(Sink.ignore)(Keep.right)

  /**
    * creates a [[akka.stream.scaladsl.Sink Sink]] that groups messages and publishes them in batches to a SQS queue using an [[com.amazonaws.services.sqs.AmazonSQSAsync AmazonSQSAsync]]
    * @see https://doc.akka.io/docs/akka/current/stream/operators/Source-or-Flow/groupedWithin.html#groupedwithin
   */
  def groupedMessageSink(queueUrl: String, settings: SqsPublishGroupedSettings = SqsPublishGroupedSettings.Defaults)(
      implicit sqsClient: AmazonSQSAsync
  ): Sink[SendMessageRequest, Future[Done]] =
    SqsPublishFlow.grouped(queueUrl, settings).toMat(Sink.ignore)(Keep.right)

  /**
    * creates a [[akka.stream.scaladsl.Sink Sink]] to publish messages in batches to a SQS queue using an [[com.amazonaws.services.sqs.AmazonSQSAsync AmazonSQSAsync]]
   */
  def batchedMessageSink(queueUrl: String, settings: SqsPublishBatchSettings = SqsPublishBatchSettings.Defaults)(
      implicit sqsClient: AmazonSQSAsync
  ): Sink[Iterable[SendMessageRequest], Future[Done]] =
    SqsPublishFlow.batch(queueUrl, settings).toMat(Sink.ignore)(Keep.right)
}
