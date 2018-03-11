/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.sqs.scaladsl

import akka.Done
import akka.stream.alpakka.sqs.{MessageActionPair, SqsAckSinkSettings}
import akka.stream.scaladsl.{Keep, Sink}
import com.amazonaws.services.sqs.AmazonSQSAsync

import scala.concurrent.Future

/**
 * Scala API to create acknowledging SQS sinks.
 */
object SqsAckSink {

  /**
   * Creates a sink for a SQS queue using an [[com.amazonaws.services.sqs.AmazonSQSAsync]].
   */
  def apply(queueUrl: String, settings: SqsAckSinkSettings = SqsAckSinkSettings.Defaults)(
      implicit sqsClient: AmazonSQSAsync
  ): Sink[MessageActionPair, Future[Done]] =
    SqsAckFlow.apply(queueUrl, settings).toMat(Sink.ignore)(Keep.right)

}
