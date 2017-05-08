/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.sqs.javadsl

import java.util.concurrent.CompletionStage
import akka.Done
import akka.stream.alpakka.sqs.{SqsFlowStage, SqsSinkSettings, _}
import akka.stream.javadsl.Sink
import com.amazonaws.services.sqs.AmazonSQSAsync
import scala.compat.java8.FutureConverters.FutureOps

object SqsSink {

  /**
   * Java API: creates a sink based on [[SqsFlowStage]] for a SQS queue using an [[AmazonSQSAsync]]
   */
  def create(queueUrl: String,
             settings: SqsSinkSettings,
             sqsClient: AmazonSQSAsync): Sink[String, CompletionStage[Done]] =
    scaladsl.SqsSink.apply(queueUrl, settings)(sqsClient).mapMaterializedValue(_.toJava).asJava

  /**
   * Java API: creates a sink based on [[SqsFlowStage]] for a SQS queue using an [[AmazonSQSAsync]] with default settings.
   */
  def create(queueUrl: String, sqsClient: AmazonSQSAsync): Sink[String, CompletionStage[Done]] =
    create(queueUrl, SqsSinkSettings.Defaults, sqsClient)
}
