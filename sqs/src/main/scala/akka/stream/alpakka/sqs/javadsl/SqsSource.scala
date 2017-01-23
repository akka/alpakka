/*
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.sqs.javadsl

import akka.NotUsed
import akka.stream.alpakka.sqs.{SqsSourceSettings, SqsSourceStage}
import akka.stream.javadsl.Source
import com.amazonaws.services.sqs.AmazonSQSAsyncClient
import com.amazonaws.services.sqs.model.Message

import scala.concurrent.duration._

object SqsSource {

  /**
   * Java API: creates a [[SqsSourceStage]] for a SQS queue using an [[AmazonSQSAsyncClient]]
   */
  def create(queueUrl: String, settings: SqsSourceSettings): Source[Message, NotUsed] =
    Source.fromGraph(new SqsSourceStage(queueUrl, settings))

  /**
   * Java API: creates a [[SqsSourceStage]] for a SQS queue using an [[AmazonSQSAsyncClient]] with default settings.
   */
  def create(queueUrl: String): Source[Message, NotUsed] =
    Source.fromGraph(new SqsSourceStage(queueUrl, SqsSourceSettings.Defaults))
}
