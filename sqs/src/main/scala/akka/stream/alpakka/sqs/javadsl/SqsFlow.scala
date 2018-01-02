/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.sqs.javadsl

import akka.NotUsed
import akka.stream.alpakka.sqs.scaladsl.Result
import akka.stream.alpakka.sqs._
import akka.stream.javadsl.Flow
import akka.stream.scaladsl.{Flow => SFlow}
import com.amazonaws.services.sqs.AmazonSQSAsync
import java.lang.{Iterable => JIterable}
import scala.collection.JavaConverters._

object SqsFlow {

  /**
   * Java API: creates a flow based on [[SqsFlowStage]] for a SQS queue using an [[AmazonSQSAsync]]
   */
  def create(queueUrl: String, settings: SqsSinkSettings, sqsClient: AmazonSQSAsync): Flow[String, Result, NotUsed] =
    scaladsl.SqsFlow.apply(queueUrl, settings)(sqsClient).asJava

  /**
   * Java API: creates a flow based on [[SqsBatchFlowStage]] for a SQS queue using an [[AmazonSQSAsync]]
   */
  def grouped(queueUrl: String,
              settings: SqsBatchFlowSettings,
              sqsClient: AmazonSQSAsync): Flow[String, Result, NotUsed] =
    scaladsl.SqsFlow.grouped(queueUrl, settings)(sqsClient).asJava

  /**
   * Java API: creates a flow based on [[SqsBatchFlowStage]] for a SQS queue using an [[AmazonSQSAsync]]
   */
  def batch(queueUrl: String,
            settings: SqsBatchFlowSettings,
            sqsClient: AmazonSQSAsync): Flow[Seq[String], Seq[Result], NotUsed] =
    scaladsl.SqsFlow.batch(queueUrl, settings)(sqsClient).asJava

  /**
   * Java API: creates a flow based on [[SqsFlowStage]] for a SQS queue using an [[AmazonSQSAsync]] with default settings.
   */
  def create(queueUrl: String, sqsClient: AmazonSQSAsync): Flow[String, Result, NotUsed] =
    create(queueUrl, SqsSinkSettings.Defaults, sqsClient)

  /**
   * Java API: creates a flow based on [[SqsBatchFlowStage]] for a SQS queue using an [[AmazonSQSAsync]] with default settings
   */
  def grouped(queueUrl: String, sqsClient: AmazonSQSAsync): Flow[String, Result, NotUsed] =
    grouped(queueUrl, SqsBatchFlowSettings.Defaults, sqsClient)

  /**
   * Java API: creates a flow based on [[SqsBatchFlowStage]] for a SQS queue using an [[AmazonSQSAsync]] with default settings
   */
  def batch(queueUrl: String, sqsClient: AmazonSQSAsync): Flow[JIterable[String], JIterable[Result], NotUsed] =
    SFlow[JIterable[String]]
      .map(jIterable => jIterable.asScala)
      .via(scaladsl.SqsFlow.batch(queueUrl, SqsBatchFlowSettings.Defaults)(sqsClient))
      .map(sIterable => sIterable.asJava)
      .asJava
}
