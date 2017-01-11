/*
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.sqs.scaladsl

import akka.Done
import akka.stream.alpakka.sqs.{ SqsSinkSettings, SqsSinkStage }
import akka.stream.scaladsl.Sink
import com.amazonaws.services.sqs.AmazonSQSAsync

import scala.concurrent.Future

object SqsSink {

  /**
   * Scala API: creates a [[SqsSinkStage]] for a SQS queue using an [[AmazonSQSAsync]]
   */
  def apply(queueUrl: String, settings: SqsSinkSettings = SqsSinkSettings.Defaults)(
      implicit sqsClient: AmazonSQSAsync): Sink[String, Future[Done]] =
    Sink.fromGraph(new SqsSinkStage(queueUrl, settings, sqsClient))
}
