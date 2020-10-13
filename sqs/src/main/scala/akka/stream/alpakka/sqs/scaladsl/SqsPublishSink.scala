/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.sqs.scaladsl

import akka.Done
import akka.stream.alpakka.sqs.{SqsPublishBatchSettings, SqsPublishGroupedSettings, SqsPublishSettings}
import akka.stream.scaladsl.{Flow, Keep, Sink}
import software.amazon.awssdk.services.sqs.SqsAsyncClient
import software.amazon.awssdk.services.sqs.model.SendMessageRequest

import scala.concurrent.Future

/**
 * Scala API to create publishing SQS sinks.
 */
object SqsPublishSink {

  /**
   * creates a [[akka.stream.scaladsl.Sink Sink]] that accepts strings and publishes them as messages to a SQS queue using an [[software.amazon.awssdk.services.sqs.SqsAsyncClient SqsAsyncClient]]
   */
  def apply(
      queueUrl: String,
      settings: SqsPublishSettings = SqsPublishSettings.Defaults
  )(implicit sqsClient: SqsAsyncClient): Sink[String, Future[Done]] =
    Flow
      .fromFunction((msg: String) => SendMessageRequest.builder().queueUrl(queueUrl).messageBody(msg).build())
      .toMat(messageSink(queueUrl, settings))(Keep.right)

  /**
   * creates a [[akka.stream.scaladsl.Sink Sink]] that groups strings and publishes them as messages in batches to a SQS queue using an [[software.amazon.awssdk.services.sqs.SqsAsyncClient SqsAsyncClient]]
   *
   * @see https://doc.akka.io/docs/akka/current/stream/operators/Source-or-Flow/groupedWithin.html#groupedwithin
   */
  def grouped(queueUrl: String, settings: SqsPublishGroupedSettings = SqsPublishGroupedSettings.Defaults)(implicit
      sqsClient: SqsAsyncClient
  ): Sink[String, Future[Done]] =
    Flow
      .fromFunction((msg: String) => SendMessageRequest.builder().queueUrl(queueUrl).messageBody(msg).build())
      .toMat(groupedMessageSink(queueUrl, settings))(Keep.right)

  /**
   * creates a [[akka.stream.scaladsl.Sink Sink]] that accepts an iterable of strings and publish them as messages in batches to a SQS queue using an [[software.amazon.awssdk.services.sqs.SqsAsyncClient SqsAsyncClient]]
   *
   * @see https://doc.akka.io/docs/akka/current/stream/operators/Source-or-Flow/groupedWithin.html#groupedwithin
   */
  def batch(
      queueUrl: String,
      settings: SqsPublishBatchSettings = SqsPublishBatchSettings.Defaults
  )(implicit sqsClient: SqsAsyncClient): Sink[Iterable[String], Future[Done]] =
    Flow
      .fromFunction((msgs: Iterable[String]) =>
        msgs.map(msg => SendMessageRequest.builder().queueUrl(queueUrl).messageBody(msg).build())
      )
      .toMat(batchedMessageSink(queueUrl, settings))(Keep.right)

  /**
   * creates a [[akka.stream.scaladsl.Sink Sink]] to publish messages to a SQS queue using an [[software.amazon.awssdk.services.sqs.SqsAsyncClient SqsAsyncClient]]
   */
  def messageSink(
      queueUrl: String,
      settings: SqsPublishSettings
  )(implicit sqsClient: SqsAsyncClient): Sink[SendMessageRequest, Future[Done]] =
    SqsPublishFlow.apply(queueUrl, settings).toMat(Sink.ignore)(Keep.right)

  /**
   * creates a [[akka.stream.scaladsl.Sink Sink]] to publish messages to a SQS queue using an [[software.amazon.awssdk.services.sqs.SqsAsyncClient SqsAsyncClient]]
   */
  def messageSink(queueUrl: String)(implicit sqsClient: SqsAsyncClient): Sink[SendMessageRequest, Future[Done]] =
    SqsPublishFlow.apply(queueUrl, SqsPublishSettings.Defaults).toMat(Sink.ignore)(Keep.right)

  /**
   * creates a [[akka.stream.scaladsl.Sink Sink]] to publish messages to SQS queues based on the message queue url using an [[software.amazon.awssdk.services.sqs.SqsAsyncClient SqsAsyncClient]]
   */
  def messageSink(
      settings: SqsPublishSettings = SqsPublishSettings.Defaults
  )(implicit sqsClient: SqsAsyncClient): Sink[SendMessageRequest, Future[Done]] =
    SqsPublishFlow.apply(settings).toMat(Sink.ignore)(Keep.right)

  /**
   * creates a [[akka.stream.scaladsl.Sink Sink]] that groups messages and publishes them in batches to a SQS queue using an [[software.amazon.awssdk.services.sqs.SqsAsyncClient SqsAsyncClient]]
   *
   * @see https://doc.akka.io/docs/akka/current/stream/operators/Source-or-Flow/groupedWithin.html#groupedwithin
   */
  def groupedMessageSink(
      queueUrl: String,
      settings: SqsPublishGroupedSettings = SqsPublishGroupedSettings.Defaults
  )(implicit sqsClient: SqsAsyncClient): Sink[SendMessageRequest, Future[Done]] =
    SqsPublishFlow.grouped(queueUrl, settings).toMat(Sink.ignore)(Keep.right)

  /**
   * creates a [[akka.stream.scaladsl.Sink Sink]] to publish messages in batches to a SQS queue using an [[software.amazon.awssdk.services.sqs.SqsAsyncClient SqsAsyncClient]]
   */
  def batchedMessageSink(
      queueUrl: String,
      settings: SqsPublishBatchSettings = SqsPublishBatchSettings.Defaults
  )(implicit sqsClient: SqsAsyncClient): Sink[Iterable[SendMessageRequest], Future[Done]] =
    SqsPublishFlow.batch(queueUrl, settings).toMat(Sink.ignore)(Keep.right)
}
