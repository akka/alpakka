/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.sqs.javadsl

import akka.NotUsed
import akka.stream.alpakka.sqs._
import akka.stream.alpakka.sqs.scaladsl.AckResult
import akka.stream.javadsl.Flow
import com.amazonaws.services.sqs.AmazonSQSAsync

object SqsAckFlow {

  /**
   * Java API: creates an acknowledging flow based on [[SqsAckFlowStage]] for a SQS queue using an [[AmazonSQSAsync]]
   */
  def create(queueUrl: String,
             settings: SqsAckSinkSettings,
             sqsClient: AmazonSQSAsync): Flow[MessageActionPair, AckResult, NotUsed] =
    scaladsl.SqsAckFlow.apply(queueUrl, settings)(sqsClient).asJava

  /**
   * Java API: creates an acknowledging flow based on [[SqsAckFlowStage]] for a SQS queue using an [[AmazonSQSAsync]]
   */
  def create(queueUrl: String, sqsClient: AmazonSQSAsync): Flow[MessageActionPair, AckResult, NotUsed] =
    create(queueUrl, SqsAckSinkSettings.Defaults, sqsClient)

}
