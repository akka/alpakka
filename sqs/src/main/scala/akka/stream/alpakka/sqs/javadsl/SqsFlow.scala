/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.sqs.javadsl

import akka.NotUsed
import akka.stream.alpakka.sqs.scaladsl.Result
import akka.stream.alpakka.sqs._
import akka.stream.javadsl.Flow
import akka.stream.scaladsl.{Flow => SFlow}
import com.amazonaws.services.sqs.AmazonSQSAsync
import java.lang.{Iterable => JIterable}

import com.amazonaws.services.sqs.model.SendMessageRequest

import scala.collection.JavaConverters._

/**
 * Java API to create SQS flows.
 */
object SqsFlow {

  /**
   * Creates a flow for a SQS queue using an [[com.amazonaws.services.sqs.AmazonSQSAsync]].
   */
  def create(queueUrl: String,
             settings: SqsSinkSettings,
             sqsClient: AmazonSQSAsync): Flow[SendMessageRequest, Result, NotUsed] =
    scaladsl.SqsFlow.apply(queueUrl, settings)(sqsClient).asJava

  /**
   * Creates a flow for a SQS queue using an [[com.amazonaws.services.sqs.AmazonSQSAsync]].
   */
  def grouped(queueUrl: String,
              settings: SqsBatchFlowSettings,
              sqsClient: AmazonSQSAsync): Flow[SendMessageRequest, Result, NotUsed] =
    scaladsl.SqsFlow.grouped(queueUrl, settings)(sqsClient).asJava

  /**
   * Creates a flow for a SQS queue using an [[com.amazonaws.services.sqs.AmazonSQSAsync]].
   */
  def batch(queueUrl: String,
            settings: SqsBatchFlowSettings,
            sqsClient: AmazonSQSAsync): Flow[Seq[SendMessageRequest], Seq[Result], NotUsed] =
    scaladsl.SqsFlow.batch(queueUrl, settings)(sqsClient).asJava

  /**
   * Creates a flow for a SQS queue using an [[com.amazonaws.services.sqs.AmazonSQSAsync]] with default settings.
   */
  def create(queueUrl: String, sqsClient: AmazonSQSAsync): Flow[SendMessageRequest, Result, NotUsed] =
    create(queueUrl, SqsSinkSettings.Defaults, sqsClient)

  /**
   * Creates a flow for a SQS queue using an [[com.amazonaws.services.sqs.AmazonSQSAsync]] with default settings.
   */
  def grouped(queueUrl: String, sqsClient: AmazonSQSAsync): Flow[SendMessageRequest, Result, NotUsed] =
    grouped(queueUrl, SqsBatchFlowSettings.Defaults, sqsClient)

  /**
   * Creates a flow for a SQS queue using an [[com.amazonaws.services.sqs.AmazonSQSAsync]] with default settings.
   */
  def batch(queueUrl: String,
            sqsClient: AmazonSQSAsync): Flow[JIterable[SendMessageRequest], JIterable[Result], NotUsed] =
    SFlow[JIterable[SendMessageRequest]]
      .map(jIterable => jIterable.asScala)
      .via(scaladsl.SqsFlow.batch(queueUrl, SqsBatchFlowSettings.Defaults)(sqsClient))
      .map(sIterable => sIterable.asJava)
      .asJava
}
