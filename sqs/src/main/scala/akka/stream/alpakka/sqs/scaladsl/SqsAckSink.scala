/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.sqs.scaladsl

import akka.Done
import akka.stream.alpakka.sqs.{MessageActionPair, SqsAckFlowStage, SqsAckSinkSettings}
import akka.stream.scaladsl.{Keep, Sink}
import com.amazonaws.services.sqs.AmazonSQSAsync
import scala.concurrent.Future

object SqsAckSink {

  /**
   * Scala API: creates a sink based on [[SqsAckFlowStage]] for a SQS queue using an [[com.amazonaws.services.sqs.AmazonSQSAsync]]
   */
  def apply(queueUrl: String, settings: SqsAckSinkSettings = SqsAckSinkSettings.Defaults)(
      implicit sqsClient: AmazonSQSAsync
  ): Sink[MessageActionPair, Future[Done]] =
    SqsAckFlow.apply(queueUrl, settings).toMat(Sink.ignore)(Keep.right)
}
