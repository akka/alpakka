/*
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.sqs.javadsl

import akka.Done
import akka.stream.alpakka.sqs.{ SqsSinkSettings, SqsSinkStage, SqsSourceStage }
import akka.stream.javadsl.Sink
import com.amazonaws.services.sqs.AmazonSQSAsync

import scala.concurrent.Future

object SqsSink {

  /**
   * Java API: creates a [[SqsSinkStage]] for a SQS queue using an [[AmazonSQSAsync]]
   */
  def create(queueUrl: String, settings: SqsSinkSettings, sqsClient: AmazonSQSAsync): Sink[String, Future[Done]] =
    Sink.fromGraph(new SqsSinkStage(queueUrl, settings, sqsClient))

  /**
   * Java API: creates a [[SqsSourceStage]] for a SQS queue using an [[AmazonSQSAsync]] with default settings.
   */
  def create(queueUrl: String, sqsClient: AmazonSQSAsync): Sink[String, Future[Done]] =
    Sink.fromGraph(new SqsSinkStage(queueUrl, SqsSinkSettings.Defaults, sqsClient))
}
