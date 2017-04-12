/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.sqs.scaladsl

import akka.NotUsed
import akka.stream.alpakka.sqs._
import akka.stream.scaladsl.Flow
import com.amazonaws.services.sqs.AmazonSQSAsync
import com.amazonaws.{AmazonWebServiceResult, ResponseMetadata}

object SqsAckFlow {

  /**
   * Scala API: creates a [[SqsAckFlowStage]] for a SQS queue using an [[AmazonSQSAsync]]
   */
  def apply(queueUrl: String, settings: SqsAckSinkSettings = SqsAckSinkSettings.Defaults)(
      implicit sqsClient: AmazonSQSAsync
  ): Flow[MessageActionPair, AckResult, NotUsed] =
    Flow.fromGraph(new SqsAckFlowStage(queueUrl, sqsClient)).mapAsync(settings.maxInFlight)(identity)
}

/**
 * Messages returned by a SqsFlow.
 * @param metadata metadata with AWS response details.
 * @param message message body.
 */
final case class AckResult(
    metadata: AmazonWebServiceResult[ResponseMetadata],
    message: String
)
