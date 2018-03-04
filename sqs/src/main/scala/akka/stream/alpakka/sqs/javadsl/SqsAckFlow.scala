/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.sqs.javadsl

import akka.NotUsed
import akka.stream.alpakka.sqs._
import akka.stream.alpakka.sqs.scaladsl.AckResult
import akka.stream.javadsl.Flow
import com.amazonaws.services.sqs.AmazonSQSAsync

/**
 * Java API to create acknowledging SQS flows.
 */
object SqsAckFlow {

  /**
   * Creates an acknowledging flow for a SQS queue using an [[com.amazonaws.services.sqs.AmazonSQSAsync]].
   */
  def create(queueUrl: String,
             settings: SqsAckSinkSettings,
             sqsClient: AmazonSQSAsync): Flow[MessageActionPair, AckResult, NotUsed] =
    scaladsl.SqsAckFlow.apply(queueUrl, settings)(sqsClient).asJava

  /**
   * Creates an acknowledging flow for a SQS queue using an [[com.amazonaws.services.sqs.AmazonSQSAsync]].
   */
  def create(queueUrl: String, sqsClient: AmazonSQSAsync): Flow[MessageActionPair, AckResult, NotUsed] =
    create(queueUrl, SqsAckSinkSettings.Defaults, sqsClient)

  /**
   * Creates an acknowledging flow for a SQS queue using an [[com.amazonaws.services.sqs.AmazonSQSAsync]].
   */
  def grouped(queueUrl: String,
              settings: SqsBatchAckFlowSettings,
              sqsClient: AmazonSQSAsync): Flow[MessageActionPair, AckResult, NotUsed] =
    scaladsl.SqsAckFlow.grouped(queueUrl, settings)(sqsClient).asJava

  /**
   * Creates an acknowledging flow for a SQS queue using an [[com.amazonaws.services.sqs.AmazonSQSAsync]].
   */
  def grouped(queueUrl: String, sqsClient: AmazonSQSAsync): Flow[MessageActionPair, AckResult, NotUsed] =
    grouped(queueUrl, SqsBatchAckFlowSettings.Defaults, sqsClient)

}
