/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.sqs.scaladsl

import akka._
import akka.stream._
import akka.stream.alpakka.sqs.SqsSourceSettings
import akka.stream.alpakka.sqs.impl.BalancingMapAsync
import akka.stream.scaladsl.{Flow, Source}
import software.amazon.awssdk.services.sqs.SqsAsyncClient
import software.amazon.awssdk.services.sqs.model._

import scala.collection.JavaConverters._
import scala.compat.java8.FutureConverters._

/**
 * Scala API to create SQS sources.
 */
object SqsSource {

  /**
   * creates a [[akka.stream.scaladsl.Source Source]] for a SQS queue using [[software.amazon.awssdk.services.sqs.SqsAsyncClient SqsAsyncClient]]
   */
  def apply(
      queueUrl: String,
      settings: SqsSourceSettings = SqsSourceSettings.Defaults
  )(implicit sqsClient: SqsAsyncClient): Source[Message, NotUsed] =
    Source
      .repeat {
        val requestBuilder =
          ReceiveMessageRequest
            .builder()
            .queueUrl(queueUrl)
            .attributeNames(settings.attributeNames.map(_.name).map(QueueAttributeName.fromValue).asJava)
            .messageAttributeNames(settings.messageAttributeNames.map(_.name).asJava)
            .maxNumberOfMessages(settings.maxBatchSize)
            .waitTimeSeconds(settings.waitTimeSeconds)

        settings.visibilityTimeout match {
          case None => requestBuilder.build()
          case Some(t) => requestBuilder.visibilityTimeout(t.toSeconds.toInt).build()
        }
      }
      .via(resolveHandler(settings.parallelRequests))
      .map(_.messages().asScala.toList)
      .takeWhile(messages => !settings.closeOnEmptyReceive || messages.nonEmpty)
      .mapConcat(identity)
      .buffer(settings.maxBufferSize, OverflowStrategy.backpressure)

  private def resolveHandler(parallelism: Int)(implicit sqsClient: SqsAsyncClient) =
    if (parallelism == 1) {
      Flow[ReceiveMessageRequest].mapAsync(parallelism)(sqsClient.receiveMessage(_).toScala)
    } else {
      BalancingMapAsync[ReceiveMessageRequest, ReceiveMessageResponse](
        parallelism,
        sqsClient.receiveMessage(_).toScala,
        (response, _) => if (response.messages().isEmpty) 1 else parallelism
      )
    }
}
