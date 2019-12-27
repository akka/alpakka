/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.aws.eventbridge.javadsl

import java.util.concurrent.CompletionStage

import akka.stream.alpakka.aws.eventbridge.EventBridgePublishSettings
import akka.stream.javadsl.{Flow, Keep, Sink}
import akka.{Done, NotUsed}
import software.amazon.awssdk.services.eventbridge.EventBridgeAsyncClient
import software.amazon.awssdk.services.eventbridge.model._

/**
 * Java API
 * Amazon SNS publisher factory.
 */
object EventBridgePublisher {

  /**
   * creates a [[akka.stream.javadsl.Flow Flow]] to publish messages to an EventBridge using an [[software.amazon.awssdk.services.eventbridge.EventBridgeAsyncClient EventBridgeAsyncClient]]
   */
  def createFlow(settings: EventBridgePublishSettings,
                 eventBridgeClient: EventBridgeAsyncClient): Flow[PutEventsRequestEntry, PutEventsResponse, NotUsed] =
    akka.stream.alpakka.aws.eventbridge.scaladsl.EventBridgePublisher.flow(settings)(eventBridgeClient).asJava

  /**
   * creates a [[akka.stream.javadsl.Flow Flow]] to publish messages to an EventBridge using an [[software.amazon.awssdk.services.eventbridge.EventBridgeAsyncClient EventBridgeAsyncClient]]
   */
  def createFlow(eventBridgeClient: EventBridgeAsyncClient): Flow[PutEventsRequestEntry, PutEventsResponse, NotUsed] =
    akka.stream.alpakka.aws.eventbridge.scaladsl.EventBridgePublisher
      .flow(EventBridgePublishSettings())(eventBridgeClient)
      .asJava

  /**
   * creates a [[akka.stream.javadsl.Flow Flow]] to publish messages to an EventBridge using an [[software.amazon.awssdk.services.eventbridge.EventBridgeAsyncClient EventBridgeAsyncClient]]
   */
  def createFlowSeq(
      settings: EventBridgePublishSettings,
      eventBridgeClient: EventBridgeAsyncClient
  ): Flow[PutEventsRequestEntry, PutEventsResponse, NotUsed] =
    akka.stream.alpakka.aws.eventbridge.scaladsl.EventBridgePublisher.flow(settings)(eventBridgeClient).asJava

  /**
   * creates a [[akka.stream.javadsl.Flow Flow]] to publish messages to an EventBridge using an [[software.amazon.awssdk.services.eventbridge.EventBridgeAsyncClient EventBridgeAsyncClient]]
   */
  def createFlowSeq(
      eventBridgeClient: EventBridgeAsyncClient
  ): Flow[Seq[PutEventsRequestEntry], PutEventsResponse, NotUsed] =
    akka.stream.alpakka.aws.eventbridge.scaladsl.EventBridgePublisher
      .flowSeq(EventBridgePublishSettings())(eventBridgeClient)
      .asJava

  /**
   * creates a [[akka.stream.javadsl.Flow Flow]] to publish messages to an EventBridge using an [[software.amazon.awssdk.services.eventbridge.EventBridgeAsyncClient EventBridgeAsyncClient]]
   */
  def createPublishFlow(settings: EventBridgePublishSettings,
                        eventBridgeClient: EventBridgeAsyncClient): Flow[PutEventsRequest, PutEventsResponse, NotUsed] =
    akka.stream.alpakka.aws.eventbridge.scaladsl.EventBridgePublisher.publishFlow(settings)(eventBridgeClient).asJava

  /**
   * creates a [[akka.stream.javadsl.Flow Flow]] to publish messages using an [[software.amazon.awssdk.services.eventbridge.EventBridgeAsyncClient EventBridgeAsyncClient]]
   */
  def createPublishFlow(eventBridgeClient: EventBridgeAsyncClient): Flow[PutEventsRequest, PutEventsResponse, NotUsed] =
    akka.stream.alpakka.aws.eventbridge.scaladsl.EventBridgePublisher
      .publishFlow(EventBridgePublishSettings())(eventBridgeClient)
      .asJava

  /**
   * creates a [[akka.stream.javadsl.Sink Sink]] to publish messages to an EventBridge using an [[software.amazon.awssdk.services.eventbridge.EventBridgeAsyncClient EventBridgeAsyncClient]]
   */
  def createSink(eventBridgeClient: EventBridgeAsyncClient): Sink[PutEventsRequestEntry, CompletionStage[Done]] =
    createFlow(EventBridgePublishSettings(), eventBridgeClient)
      .toMat(Sink.ignore(), Keep.right[NotUsed, CompletionStage[Done]])

  /**
   * creates a [[akka.stream.javadsl.Sink Sink]] to publish messages to an EventBridge using an [[software.amazon.awssdk.services.eventbridge.EventBridgeAsyncClient EventBridgeAsyncClient]]
   */
  def createSink(settings: EventBridgePublishSettings,
                 eventBridgeClient: EventBridgeAsyncClient): Sink[PutEventsRequestEntry, CompletionStage[Done]] =
    createFlow(settings, eventBridgeClient)
      .toMat(Sink.ignore(), Keep.right[NotUsed, CompletionStage[Done]])

  /**
   * creates a [[akka.stream.javadsl.Sink Sink]] to publish messages to a SNS topics based on the message topic arn using an [[software.amazon.awssdk.services.eventbridge.EventBridgeAsyncClient EventBridgeAsyncClient]]
   */
  def createPublishSink(settings: EventBridgePublishSettings,
                        eventBridgeClient: EventBridgeAsyncClient): Sink[PutEventsRequest, CompletionStage[Done]] =
    createPublishFlow(settings, eventBridgeClient)
      .toMat(Sink.ignore(), Keep.right[NotUsed, CompletionStage[Done]])

  /**
   * creates a [[akka.stream.javadsl.Sink Sink]] to publish messages to a SNS topics based on the message topic arn using an [[software.amazon.awssdk.services.eventbridge.EventBridgeAsyncClient EventBridgeAsyncClient]]
   */
  def createPublishSink(eventBridgeClient: EventBridgeAsyncClient): Sink[PutEventsRequest, CompletionStage[Done]] =
    createPublishFlow(EventBridgePublishSettings(), eventBridgeClient)
      .toMat(Sink.ignore(), Keep.right[NotUsed, CompletionStage[Done]])
}
