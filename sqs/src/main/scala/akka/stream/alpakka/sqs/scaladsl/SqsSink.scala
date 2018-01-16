/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.sqs.scaladsl

import akka.Done
import akka.stream.alpakka.sqs.{SqsBatchFlowSettings, SqsFlowStage, SqsSinkSettings}
import akka.stream.scaladsl.{Flow, Keep, Sink}
import com.amazonaws.services.sqs.AmazonSQSAsync
import com.amazonaws.services.sqs.model.SendMessageRequest

import scala.concurrent.Future

object SqsSink {

  /**
   * Scala API: creates a sink based on [[SqsFlowStage]] for a SQS queue using an [[AmazonSQSAsync]]
   */
  def apply(queueUrl: String, settings: SqsSinkSettings = SqsSinkSettings.Defaults)(
      implicit sqsClient: AmazonSQSAsync
  ): Sink[String, Future[Done]] =
    Flow
      .fromFunction((msg: String) => new SendMessageRequest(queueUrl, msg))
      .via(SqsFlow.apply(queueUrl, settings))
      .toMat(Sink.ignore)(Keep.right)

  def grouped(queueUrl: String, settings: SqsBatchFlowSettings = SqsBatchFlowSettings.Defaults)(
      implicit sqsClient: AmazonSQSAsync
  ): Sink[String, Future[Done]] =
    Flow
      .fromFunction((msg: String) => new SendMessageRequest(queueUrl, msg))
      .via(SqsFlow.grouped(queueUrl, settings))
      .toMat(Sink.ignore)(Keep.right)

  def batch(queueUrl: String, settings: SqsBatchFlowSettings = SqsBatchFlowSettings.Defaults)(
      implicit sqsClient: AmazonSQSAsync
  ): Sink[Iterable[String], Future[Done]] =
    Flow
      .fromFunction((msgs: Iterable[String]) => msgs.map(msg => new SendMessageRequest(queueUrl, msg)))
      .via(SqsFlow.batch(queueUrl, settings))
      .toMat(Sink.ignore)(Keep.right)

  def messageSink(queueUrl: String, settings: SqsSinkSettings = SqsSinkSettings.Defaults)(
      implicit sqsClient: AmazonSQSAsync
  ): Sink[SendMessageRequest, Future[Done]] =
    SqsFlow.apply(queueUrl, settings).toMat(Sink.ignore)(Keep.right)

  def groupedMessageSink(queueUrl: String, settings: SqsBatchFlowSettings = SqsBatchFlowSettings.Defaults)(
      implicit sqsClient: AmazonSQSAsync
  ): Sink[SendMessageRequest, Future[Done]] =
    SqsFlow.grouped(queueUrl, settings).toMat(Sink.ignore)(Keep.right)

  def batchedMessageSink(queueUrl: String, settings: SqsBatchFlowSettings = SqsBatchFlowSettings.Defaults)(
      implicit sqsClient: AmazonSQSAsync
  ): Sink[Iterable[SendMessageRequest], Future[Done]] =
    SqsFlow.batch(queueUrl, settings).toMat(Sink.ignore)(Keep.right)
}
