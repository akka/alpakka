/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.sqs.javadsl

import akka.NotUsed
import akka.annotation.ApiMayChange
import akka.stream.alpakka.sqs._
import akka.stream.javadsl.Flow
import software.amazon.awssdk.core.SdkPojo
import software.amazon.awssdk.services.sqs.SqsAsyncClient
import software.amazon.awssdk.services.sqs.model.SqsResponse

/**
 * Java API to create acknowledging SQS flows.
 */
@ApiMayChange
object SqsAckFlow {

  /**
   * creates a [[akka.stream.javadsl.Flow Flow]] for ack a single SQS message at a time using an [[software.amazon.awssdk.services.sqs.SqsAsyncClient]].
   */
  def create(queueUrl: String,
             settings: SqsAckSettings,
             sqsClient: SqsAsyncClient): Flow[MessageAction, SqsAckResult[SqsResponse], NotUsed] =
    akka.stream.alpakka.sqs.scaladsl.SqsAckFlow.apply(queueUrl, settings)(sqsClient).asJava

  /**
   * creates a [[akka.stream.javadsl.Flow Flow]] for ack grouped SQS messages using an [[software.amazon.awssdk.services.sqs.SqsAsyncClient]].
   */
  def grouped(queueUrl: String,
              settings: SqsAckGroupedSettings,
              sqsClient: SqsAsyncClient): Flow[MessageAction, SqsAckResult[SdkPojo], NotUsed] =
    akka.stream.alpakka.sqs.scaladsl.SqsAckFlow.grouped(queueUrl, settings)(sqsClient).asJava
}
