/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.sqs.scaladsl

import akka.Done
import akka.stream.alpakka.sqs.{SqsBatchFlowSettings, SqsFlowStage, SqsSinkSettings}
import akka.stream.scaladsl.{Keep, Sink}
import com.amazonaws.services.sqs.AmazonSQSAsync

import scala.concurrent.Future

object SqsSink {

  /**
   * Scala API: creates a sink based on [[SqsFlowStage]] for a SQS queue using an [[AmazonSQSAsync]]
   */
  def apply(queueUrl: String, settings: SqsSinkSettings = SqsSinkSettings.Defaults)(
      implicit sqsClient: AmazonSQSAsync
  ): Sink[String, Future[Done]] =
    SqsFlow.apply(queueUrl, settings).toMat(Sink.ignore)(Keep.right)

  def grouped(queueUrl: String, settings: SqsBatchFlowSettings = SqsBatchFlowSettings.Defaults)(
      implicit sqsClient: AmazonSQSAsync
  ): Sink[String, Future[Done]] =
    SqsFlow.grouped(queueUrl, settings).toMat(Sink.ignore)(Keep.right)

  def batch(queueUrl: String, settings: SqsBatchFlowSettings = SqsBatchFlowSettings.Defaults)(
      implicit sqsClient: AmazonSQSAsync
  ): Sink[Iterable[String], Future[Done]] =
    SqsFlow.batch(queueUrl, settings).toMat(Sink.ignore)(Keep.right)
}
