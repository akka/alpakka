/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.sqs.scaladsl

import akka.NotUsed
import akka.stream.alpakka.sqs.{SqsFlowStage, SqsSinkSettings}
import akka.stream.scaladsl.Flow
import com.amazonaws.services.sqs.AmazonSQSAsync
import com.amazonaws.services.sqs.model.SendMessageResult

object SqsFlow {

  /**
   * Scala API: creates a [[SqsFlowStage]] for a SQS queue using an [[AmazonSQSAsync]]
   */
  def apply(queueUrl: String, settings: SqsSinkSettings = SqsSinkSettings.Defaults)(
      implicit sqsClient: AmazonSQSAsync
  ): Flow[String, Result, NotUsed] =
    Flow.fromGraph(new SqsFlowStage(queueUrl, sqsClient)).mapAsync(settings.maxInFlight)(identity)
}

/**
 * Messages returned by a SqsFlow.
 * @param metadata metadata with AWS response details.
 * @param message message body.
 */
final case class Result(
    metadata: SendMessageResult,
    message: String
)
