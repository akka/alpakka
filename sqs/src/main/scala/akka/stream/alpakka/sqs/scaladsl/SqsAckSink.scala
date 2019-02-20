/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.sqs.scaladsl

import akka.Done
import akka.stream.alpakka.sqs.{MessageAction, SqsAckGroupedSettings, SqsAckSettings}
import akka.stream.scaladsl.{Keep, Sink}
import software.amazon.awssdk.services.sqs.SqsAsyncClient

import scala.concurrent.Future

/**
 * Scala API to create acknowledging SQS sinks.
 */
object SqsAckSink {

  /**
   * creates a [[akka.stream.scaladsl.Sink Sink]] for ack a single SQS message at a time using an [[software.amazon.awssdk.services.sqs.SqsAsyncClient]].
   */
  def apply(queueUrl: String, settings: SqsAckSettings = SqsAckSettings.Defaults)(
      implicit sqsClient: SqsAsyncClient
  ): Sink[MessageAction, Future[Done]] =
    SqsAckFlow.apply(queueUrl, settings).toMat(Sink.ignore)(Keep.right)

  /**
   * creates a [[akka.stream.scaladsl.Sink Sink]] for ack grouped SQS messages using an [[software.amazon.awssdk.services.sqs.SqsAsyncClient]].
   */
  def grouped(queueUrl: String, settings: SqsAckGroupedSettings = SqsAckGroupedSettings.Defaults)(
      implicit sqsClient: SqsAsyncClient
  ): Sink[MessageAction, Future[Done]] =
    SqsAckFlow.grouped(queueUrl, settings).toMat(Sink.ignore)(Keep.right)
}
