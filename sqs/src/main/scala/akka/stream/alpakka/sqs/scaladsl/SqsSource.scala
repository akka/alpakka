/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.sqs.scaladsl

import akka.NotUsed
import akka.stream.alpakka.sqs.{SqsSourceSettings, SqsSourceStage}
import akka.stream.scaladsl.Source
import com.amazonaws.services.sqs.AmazonSQSAsync
import com.amazonaws.services.sqs.model.Message

object SqsSource {

  /**
   * Scala API: creates a [[SqsSourceStage]] for a SQS queue.
   */
  def apply(queueUrl: String, settings: SqsSourceSettings = SqsSourceSettings.Defaults)(
      implicit sqs: AmazonSQSAsync
  ): Source[Message, NotUsed] =
    Source.fromGraph(new SqsSourceStage(queueUrl, settings))

}
