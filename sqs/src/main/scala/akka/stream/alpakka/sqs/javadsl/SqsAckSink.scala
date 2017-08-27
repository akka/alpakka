/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.sqs.javadsl

import java.util.concurrent.CompletionStage
import akka.Done
import akka.stream.alpakka.sqs.{scaladsl, MessageActionPair, SqsAckFlowStage, SqsAckSinkSettings}
import akka.stream.javadsl.Sink
import com.amazonaws.services.sqs.AmazonSQSAsync
import scala.compat.java8.FutureConverters.FutureOps

object SqsAckSink {

  /**
   * Java API: creates a sink based on [[SqsAckFlowStage]] for a SQS queue using an [[com.amazonaws.services.sqs.AmazonSQSAsync]]
   */
  def create(queueUrl: String,
             settings: SqsAckSinkSettings,
             sqsClient: AmazonSQSAsync): Sink[MessageActionPair, CompletionStage[Done]] =
    scaladsl.SqsAckSink.apply(queueUrl, settings)(sqsClient).mapMaterializedValue(_.toJava).asJava

  /**
   * Java API: creates a sink based on [[SqsAckFlowStage]] for a SQS queue using an [[com.amazonaws.services.sqs.AmazonSQSAsync]] with default settings.
   */
  def create(queueUrl: String, sqsClient: AmazonSQSAsync): Sink[MessageActionPair, CompletionStage[Done]] =
    create(queueUrl, SqsAckSinkSettings.Defaults, sqsClient)
}
