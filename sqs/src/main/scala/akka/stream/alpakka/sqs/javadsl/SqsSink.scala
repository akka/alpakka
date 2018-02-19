/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.sqs.javadsl

import java.util.concurrent.CompletionStage

import akka.Done
import akka.stream.alpakka.sqs._
import akka.stream.javadsl.Sink
import com.amazonaws.services.sqs.AmazonSQSAsync
import akka.stream.scaladsl.{Flow, Keep}
import java.lang.{Iterable => JIterable}

import com.amazonaws.services.sqs.model.SendMessageRequest

import scala.collection.JavaConverters._
import scala.compat.java8.FutureConverters.FutureOps

/**
 * Java API to create SQS Sinks.
 */
object SqsSink {

  /**
   * Creates a sink for a SQS queue using an [[com.amazonaws.services.sqs.AmazonSQSAsync]].
   */
  def create(queueUrl: String,
             settings: SqsSinkSettings,
             sqsClient: AmazonSQSAsync): Sink[String, CompletionStage[Done]] =
    scaladsl.SqsSink.apply(queueUrl, settings)(sqsClient).mapMaterializedValue(_.toJava).asJava

  /**
   * Creates a grouped sink running in batch mode for a SQS queue using an [[com.amazonaws.services.sqs.AmazonSQSAsync]].
   */
  def grouped(queueUrl: String,
              settings: SqsBatchFlowSettings,
              sqsClient: AmazonSQSAsync): Sink[String, CompletionStage[Done]] =
    scaladsl.SqsSink.grouped(queueUrl, settings)(sqsClient).mapMaterializedValue(_.toJava).asJava

  /**
   * Creates a sink running in batch mode for a SQS queue using an [[com.amazonaws.services.sqs.AmazonSQSAsync]].
   */
  def batch(queueUrl: String,
            settings: SqsBatchFlowSettings,
            sqsClient: AmazonSQSAsync): Sink[Seq[String], CompletionStage[Done]] =
    scaladsl.SqsSink.batch(queueUrl, settings)(sqsClient).mapMaterializedValue(_.toJava).asJava

  /**
   * Creates a sink for a SQS queue using an [[com.amazonaws.services.sqs.AmazonSQSAsync]] with default settings.
   */
  def create(queueUrl: String, sqsClient: AmazonSQSAsync): Sink[String, CompletionStage[Done]] =
    create(queueUrl, SqsSinkSettings.Defaults, sqsClient)

  /**
   * Creates a sink running in batch mode for a SQS queue using an [[com.amazonaws.services.sqs.AmazonSQSAsync]] with default settings.
   */
  def grouped(queueUrl: String, sqsClient: AmazonSQSAsync): Sink[String, CompletionStage[Done]] =
    scaladsl.SqsSink.grouped(queueUrl, SqsBatchFlowSettings.Defaults)(sqsClient).mapMaterializedValue(_.toJava).asJava

  /**
   * Creates a sink running in batch mode for a SQS queue using an [[com.amazonaws.services.sqs.AmazonSQSAsync]] with default settings.
   */
  def batch(queueUrl: String, sqsClient: AmazonSQSAsync): Sink[JIterable[String], CompletionStage[Done]] =
    Flow[JIterable[String]]
      .map(jIterable => jIterable.asScala)
      .toMat(scaladsl.SqsSink.batch(queueUrl, SqsBatchFlowSettings.Defaults)(sqsClient))(Keep.right)
      .mapMaterializedValue(_.toJava)
      .asJava

  /**
   * Java API: creates a sink for a SQS queue using an [[com.amazonaws.services.sqs.AmazonSQSAsync AmazonSQSAsync]]
   */
  def messageSink(queueUrl: String,
                  settings: SqsSinkSettings,
                  sqsClient: AmazonSQSAsync): Sink[SendMessageRequest, CompletionStage[Done]] =
    scaladsl.SqsSink.messageSink(queueUrl, settings)(sqsClient).mapMaterializedValue(_.toJava).asJava

  /**
   * Java API: creates a sink running in batch mode for a SQS queue using an [[com.amazonaws.services.sqs.AmazonSQSAsync AmazonSQSAsync]]
   */
  def groupedMessageSink(queueUrl: String,
                         settings: SqsBatchFlowSettings,
                         sqsClient: AmazonSQSAsync): Sink[SendMessageRequest, CompletionStage[Done]] =
    scaladsl.SqsSink.groupedMessageSink(queueUrl, settings)(sqsClient).mapMaterializedValue(_.toJava).asJava

  /**
   * Java API: creates a sink running in batch mode for a SQS queue using an [[com.amazonaws.services.sqs.AmazonSQSAsync AmazonSQSAsync]]
   */
  def batchedMessageSink(queueUrl: String,
                         settings: SqsBatchFlowSettings,
                         sqsClient: AmazonSQSAsync): Sink[Seq[SendMessageRequest], CompletionStage[Done]] =
    scaladsl.SqsSink.batchedMessageSink(queueUrl, settings)(sqsClient).mapMaterializedValue(_.toJava).asJava

  /**
   * Java API: creates a sink for a SQS queue using an [[com.amazonaws.services.sqs.AmazonSQSAsync AmazonSQSAsync]] with default settings.
   */
  def messageSink(queueUrl: String, sqsClient: AmazonSQSAsync): Sink[SendMessageRequest, CompletionStage[Done]] =
    messageSink(queueUrl, SqsSinkSettings.Defaults, sqsClient)

  /**
   * Java API: creates a sink running in batch mode for a SQS queue using an [[com.amazonaws.services.sqs.AmazonSQSAsync AmazonSQSAsync]] with default settings.
   */
  def groupedMessageSink(queueUrl: String, sqsClient: AmazonSQSAsync): Sink[SendMessageRequest, CompletionStage[Done]] =
    scaladsl.SqsSink
      .groupedMessageSink(queueUrl, SqsBatchFlowSettings.Defaults)(sqsClient)
      .mapMaterializedValue(_.toJava)
      .asJava

  /**
   * Java API: creates a sink running in batch mode for a SQS queue using an [[com.amazonaws.services.sqs.AmazonSQSAsync AmazonSQSAsync]] with default settings.
   */
  def batchedMessageSink(queueUrl: String,
                         sqsClient: AmazonSQSAsync): Sink[JIterable[SendMessageRequest], CompletionStage[Done]] =
    Flow[JIterable[SendMessageRequest]]
      .map(jIterable => jIterable.asScala)
      .toMat(scaladsl.SqsSink.batchedMessageSink(queueUrl, SqsBatchFlowSettings.Defaults)(sqsClient))(Keep.right)
      .mapMaterializedValue(_.toJava)
      .asJava
}
