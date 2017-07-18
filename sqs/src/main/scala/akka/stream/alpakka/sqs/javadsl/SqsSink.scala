/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.sqs.javadsl

import java.util.concurrent.CompletionStage

import akka.Done
import akka.stream.alpakka.sqs._
import akka.stream.javadsl.Sink
import com.amazonaws.services.sqs.AmazonSQSAsync
import akka.stream.scaladsl.{Flow, Keep}

import java.lang.{Iterable => JIterable}
import scala.collection.JavaConverters._

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
   * Java API: creates a sink based on [[SqsBatchFlowStage]] running in batch mode for a SQS queue using an [[AmazonSQSAsync]]
   */
  def grouped(queueUrl: String,
              settings: SqsBatchFlowSettings,
              sqsClient: AmazonSQSAsync): Sink[String, CompletionStage[Done]] =
    scaladsl.SqsSink.grouped(queueUrl, settings)(sqsClient).mapMaterializedValue(_.toJava).asJava

  /**
   * Java API: creates a sink based on [[SqsBatchFlowStage]] running in batch mode for a SQS queue using an [[AmazonSQSAsync]]
   */
  def batch(queueUrl: String,
            settings: SqsBatchFlowSettings,
            sqsClient: AmazonSQSAsync): Sink[Seq[String], CompletionStage[Done]] =
    scaladsl.SqsSink.batch(queueUrl, settings)(sqsClient).mapMaterializedValue(_.toJava).asJava

  /**
   * Java API: creates a sink based on [[SqsFlowStage]] for a SQS queue using an [[AmazonSQSAsync]] with default settings.
   */
  def create(queueUrl: String, sqsClient: AmazonSQSAsync): Sink[String, CompletionStage[Done]] =
    create(queueUrl, SqsSinkSettings.Defaults, sqsClient)

  /**
   * Java API: creates a sink based on [[SqsBatchFlowStage]] running in batch mode for a SQS queue using an [[AmazonSQSAsync]] with default settings.
   */
  def grouped(queueUrl: String, sqsClient: AmazonSQSAsync): Sink[String, CompletionStage[Done]] =
    scaladsl.SqsSink.grouped(queueUrl, SqsBatchFlowSettings.Defaults)(sqsClient).mapMaterializedValue(_.toJava).asJava

  /**
   * Java API: creates a sink based on [[SqsBatchFlowStage]] running in batch mode for a SQS queue using an [[AmazonSQSAsync]] with default settings.
   */
  def batch(queueUrl: String, sqsClient: AmazonSQSAsync): Sink[JIterable[String], CompletionStage[Done]] =
    Flow[JIterable[String]]
      .map(jIterable => jIterable.asScala)
      .toMat(scaladsl.SqsSink.batch(queueUrl, SqsBatchFlowSettings.Defaults)(sqsClient))(Keep.right)
      .mapMaterializedValue(_.toJava)
      .asJava

}
