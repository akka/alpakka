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
object SqsPublishSink {

  /**
   * Creates a sink for a SQS queue using an [[com.amazonaws.services.sqs.AmazonSQSAsync]].
   */
  def create(queueUrl: String,
             settings: SqsPublishSettings,
             sqsClient: AmazonSQSAsync): Sink[String, CompletionStage[Done]] =
    scaladsl.SqsPublishSink.apply(queueUrl, settings)(sqsClient).mapMaterializedValue(_.toJava).asJava

  /**
   * Creates a sink for a SQS queue using an [[com.amazonaws.services.sqs.AmazonSQSAsync]] with default settings.
   */
  def create(queueUrl: String, sqsClient: AmazonSQSAsync): Sink[String, CompletionStage[Done]] =
    create(queueUrl, SqsPublishSettings.Defaults, sqsClient)

  /**
   * Java API: creates a sink for a SQS queue using an [[com.amazonaws.services.sqs.AmazonSQSAsync AmazonSQSAsync]]
   */
  def messageSink(queueUrl: String,
                  settings: SqsPublishSettings,
                  sqsClient: AmazonSQSAsync): Sink[SendMessageRequest, CompletionStage[Done]] =
    scaladsl.SqsPublishSink
      .messageSink(queueUrl, settings)(sqsClient)
      .mapMaterializedValue(_.toJava)
      .asJava

  /**
   * Java API: creates a sink for a SQS queue using an [[com.amazonaws.services.sqs.AmazonSQSAsync AmazonSQSAsync]] with default settings.
   */
  def messageSink(queueUrl: String, sqsClient: AmazonSQSAsync): Sink[SendMessageRequest, CompletionStage[Done]] =
    messageSink(queueUrl, SqsPublishSettings.Defaults, sqsClient)

  /**
   * Creates a grouped sink running in batch mode for a SQS queue using an [[com.amazonaws.services.sqs.AmazonSQSAsync]].
   */
  def grouped(queueUrl: String,
              settings: SqsPublishGroupedSettings,
              sqsClient: AmazonSQSAsync): Sink[String, CompletionStage[Done]] =
    scaladsl.SqsPublishSink.grouped(queueUrl, settings)(sqsClient).mapMaterializedValue(_.toJava).asJava

  /**
   * Creates a sink running in batch mode for a SQS queue using an [[com.amazonaws.services.sqs.AmazonSQSAsync]] with default settings.
   */
  def grouped(queueUrl: String, sqsClient: AmazonSQSAsync): Sink[String, CompletionStage[Done]] =
    scaladsl.SqsPublishSink
      .grouped(queueUrl, SqsPublishGroupedSettings.Defaults)(sqsClient)
      .mapMaterializedValue(_.toJava)
      .asJava

  /**
   * Java API: creates a sink running in batch mode for a SQS queue using an [[com.amazonaws.services.sqs.AmazonSQSAsync AmazonSQSAsync]]
   */
  def groupedMessageSink(queueUrl: String,
                         settings: SqsPublishGroupedSettings,
                         sqsClient: AmazonSQSAsync): Sink[SendMessageRequest, CompletionStage[Done]] =
    scaladsl.SqsPublishSink
      .groupedMessageSink(queueUrl, settings)(sqsClient)
      .mapMaterializedValue(_.toJava)
      .asJava

  /**
   * Java API: creates a sink running in batch mode for a SQS queue using an [[com.amazonaws.services.sqs.AmazonSQSAsync AmazonSQSAsync]] with default settings.
   */
  def groupedMessageSink(queueUrl: String, sqsClient: AmazonSQSAsync): Sink[SendMessageRequest, CompletionStage[Done]] =
    scaladsl.SqsPublishSink
      .groupedMessageSink(queueUrl, SqsPublishGroupedSettings.Defaults)(sqsClient)
      .mapMaterializedValue(_.toJava)
      .asJava

  /**
   * Creates a sink running in batch mode for a SQS queue using an [[com.amazonaws.services.sqs.AmazonSQSAsync]].
   */
  def batch(queueUrl: String,
            settings: SqsPublishBatchSettings,
            sqsClient: AmazonSQSAsync): Sink[JIterable[String], CompletionStage[Done]] =
    Flow[JIterable[String]]
      .map(_.asScala.toSeq)
      .toMat(scaladsl.SqsPublishSink.batch(queueUrl, settings)(sqsClient))(Keep.right)
      .mapMaterializedValue(_.toJava)
      .asJava

  /**
   * Creates a sink running in batch mode for a SQS queue using an [[com.amazonaws.services.sqs.AmazonSQSAsync]] with default settings.
   */
  def batch(queueUrl: String, sqsClient: AmazonSQSAsync): Sink[JIterable[String], CompletionStage[Done]] =
    Flow[JIterable[String]]
      .map(jIterable => jIterable.asScala)
      .toMat(scaladsl.SqsPublishSink.batch(queueUrl, SqsPublishBatchSettings.Defaults)(sqsClient))(Keep.right)
      .mapMaterializedValue(_.toJava)
      .asJava

  /**
   * Java API: creates a sink running in batch mode for a SQS queue using an [[com.amazonaws.services.sqs.AmazonSQSAsync AmazonSQSAsync]]
   */
  def batchedMessageSink(queueUrl: String,
                         settings: SqsPublishBatchSettings,
                         sqsClient: AmazonSQSAsync): Sink[JIterable[SendMessageRequest], CompletionStage[Done]] =
    Flow[JIterable[SendMessageRequest]]
      .map(jIterable => jIterable.asScala)
      .toMat(scaladsl.SqsPublishSink.batchedMessageSink(queueUrl, settings)(sqsClient))(Keep.right)
      .mapMaterializedValue(_.toJava)
      .asJava

  /**
   * Java API: creates a sink running in batch mode for a SQS queue using an [[com.amazonaws.services.sqs.AmazonSQSAsync AmazonSQSAsync]] with default settings.
   */
  def batchedMessageSink(queueUrl: String,
                         sqsClient: AmazonSQSAsync): Sink[JIterable[SendMessageRequest], CompletionStage[Done]] =
    Flow[JIterable[SendMessageRequest]]
      .map(jIterable => jIterable.asScala)
      .toMat(scaladsl.SqsPublishSink.batchedMessageSink(queueUrl, SqsPublishBatchSettings.Defaults)(sqsClient))(
        Keep.right
      )
      .mapMaterializedValue(_.toJava)
      .asJava

}
