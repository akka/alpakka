/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.sqs.javadsl

import akka.NotUsed
import akka.stream.alpakka.sqs.scaladsl.Result
import akka.stream.alpakka.sqs.{SqsFlowStage, SqsSinkSettings, _}
import akka.stream.javadsl.Flow
import com.amazonaws.services.sqs.AmazonSQSAsync

object SqsFlow {

  /**
   * Java API: creates a flow based on [[SqsFlowStage]] for a SQS queue using an [[AmazonSQSAsync]]
   */
  def create(queueUrl: String, settings: SqsSinkSettings, sqsClient: AmazonSQSAsync): Flow[String, Result, NotUsed] =
    scaladsl.SqsFlow.apply(queueUrl, settings)(sqsClient).asJava

  /**
   * Java API: creates a flow based on [[SqsFlowStage]] for a SQS queue using an [[AmazonSQSAsync]] with default settings.
   */
  def create(queueUrl: String, sqsClient: AmazonSQSAsync): Flow[String, Result, NotUsed] =
    create(queueUrl, SqsSinkSettings.Defaults, sqsClient)
}
