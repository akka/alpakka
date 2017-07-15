/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.sqs.javadsl

import akka.NotUsed
import akka.stream.alpakka.sqs.{SqsSourceSettings, SqsSourceStage}
import akka.stream.javadsl.Source
import com.amazonaws.services.sqs.AmazonSQSAsync
import com.amazonaws.services.sqs.model.Message

object SqsSource {

  /**
   * Java API: creates a [[SqsSourceStage]] for a SQS queue.
   */
  def create(queueUrl: String, settings: SqsSourceSettings, sqs: AmazonSQSAsync): Source[Message, NotUsed] =
    Source.fromGraph(new SqsSourceStage(queueUrl, settings)(sqs))

  /**
   * Java API: creates a [[SqsSourceStage]] for a SQS queue with default settings.
   */
  def create(queueUrl: String, sqs: AmazonSQSAsync): Source[Message, NotUsed] =
    Source.fromGraph(new SqsSourceStage(queueUrl, SqsSourceSettings.Defaults)(sqs))
}
