/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.sqs.scaladsl

import akka.Done
import akka.stream.alpakka.sqs.{MessageActionPair, SqsAckSinkSettings, SqsAckSinkStage}
import akka.stream.scaladsl.Sink
import com.amazonaws.services.sqs.AmazonSQSAsync

import scala.concurrent.Future

object SqsAckSink {

  /**
   * Scala API: creates a [[SqsAckSinkStage]] for a SQS queue using an [[com.amazonaws.services.sqs.AmazonSQSAsync]]
   */
  def apply(queueUrl: String, settings: SqsAckSinkSettings = SqsAckSinkSettings.Defaults)(
      implicit sqsClient: AmazonSQSAsync): Sink[MessageActionPair, Future[Done]] =
    Sink.fromGraph(new SqsAckSinkStage(queueUrl, settings, sqsClient))
}
