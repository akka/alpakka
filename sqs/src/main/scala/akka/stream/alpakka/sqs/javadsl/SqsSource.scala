/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.sqs.javadsl

import akka.NotUsed
import akka.stream.alpakka.sqs.SqsSourceSettings
import akka.stream.javadsl.Source
import software.amazon.awssdk.services.sqs.SqsAsyncClient
import software.amazon.awssdk.services.sqs.model.Message

/**
 * Java API to create SQS sources.
 */
object SqsSource {

  /**
   * creates a [[akka.stream.javadsl.Source Source]] for a SQS queue using [[software.amazon.awssdk.services.sqs.SqsAsyncClient SqsAsyncClient]]
   */
  def create(queueUrl: String, settings: SqsSourceSettings, sqs: SqsAsyncClient): Source[Message, NotUsed] =
    akka.stream.alpakka.sqs.scaladsl.SqsSource(queueUrl, settings)(sqs).asJava

}
