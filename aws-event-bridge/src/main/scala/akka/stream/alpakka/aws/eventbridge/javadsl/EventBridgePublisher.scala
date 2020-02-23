/*
 * Copyright (C) 2016-2020 Lightbend Inc. <http://www.lightbend.com>
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
 * Amazon EventBridge publisher factory.
 */
object EventBridgePublisher {

  /**
   * Creates a [[akka.stream.javadsl.Flow Flow]] to publish messages to an EventBridge.
   *
   * @param settings [[akka.stream.alpakka.aws.eventbridge.EventBridgePublishSettings]] settings for publishing
   * @param eventBridgeClient [[software.amazon.awssdk.services.eventbridge.EventBridgeAsyncClient EventBridgeAsyncClient]] client for publishing
   */
  def flow(settings: EventBridgePublishSettings,
           eventBridgeClient: EventBridgeAsyncClient): Flow[PutEventsRequestEntry, PutEventsResponse, NotUsed] =
    akka.stream.alpakka.aws.eventbridge.scaladsl.EventBridgePublisher.flow(settings)(eventBridgeClient).asJava

  /**
   * Creates a [[akka.stream.javadsl.Flow Flow]] to publish messages to an EventBridge.
   *
   * @param eventBridgeClient [[software.amazon.awssdk.services.eventbridge.EventBridgeAsyncClient EventBridgeAsyncClient]] client for publishing
   */
  def flow(eventBridgeClient: EventBridgeAsyncClient): Flow[PutEventsRequestEntry, PutEventsResponse, NotUsed] =
    akka.stream.alpakka.aws.eventbridge.scaladsl.EventBridgePublisher
      .flow(EventBridgePublishSettings())(eventBridgeClient)
      .asJava

  /**
   * Creates a [[akka.stream.javadsl.Flow Flow]] to publish messages to an EventBridge.
   *
   * @param settings [[akka.stream.alpakka.aws.eventbridge.EventBridgePublishSettings]] settings for publishing
   * @param eventBridgeClient [[software.amazon.awssdk.services.eventbridge.EventBridgeAsyncClient EventBridgeAsyncClient]] client for publishing
   */
  def flowSeq(
      settings: EventBridgePublishSettings,
      eventBridgeClient: EventBridgeAsyncClient
  ): Flow[PutEventsRequestEntry, PutEventsResponse, NotUsed] =
    akka.stream.alpakka.aws.eventbridge.scaladsl.EventBridgePublisher.flow(settings)(eventBridgeClient).asJava

  /**
   * Creates a [[akka.stream.javadsl.Flow Flow]] to publish messages to an EventBridge.
   *
   * @param eventBridgeClient [[software.amazon.awssdk.services.eventbridge.EventBridgeAsyncClient EventBridgeAsyncClient]] client for publishing
   */
  def flowSeq(
      eventBridgeClient: EventBridgeAsyncClient
  ): Flow[Seq[PutEventsRequestEntry], PutEventsResponse, NotUsed] =
    akka.stream.alpakka.aws.eventbridge.scaladsl.EventBridgePublisher
      .flowSeq(EventBridgePublishSettings())(eventBridgeClient)
      .asJava

  /**
   * Creates a [[akka.stream.javadsl.Flow Flow]] to publish messages to an EventBridge.
   *
   * @param settings [[akka.stream.alpakka.aws.eventbridge.EventBridgePublishSettings]] settings for publishing
   * @param eventBridgeClient [[software.amazon.awssdk.services.eventbridge.EventBridgeAsyncClient EventBridgeAsyncClient]] client for publishing
   */
  def publishFlow(settings: EventBridgePublishSettings,
                  eventBridgeClient: EventBridgeAsyncClient): Flow[PutEventsRequest, PutEventsResponse, NotUsed] =
    akka.stream.alpakka.aws.eventbridge.scaladsl.EventBridgePublisher.publishFlow(settings)(eventBridgeClient).asJava

  /**
   * Creates a [[akka.stream.javadsl.Flow Flow]] to publish a message to an EventBridge.
   *
   * @param eventBridgeClient [[software.amazon.awssdk.services.eventbridge.EventBridgeAsyncClient EventBridgeAsyncClient]] client for publishing
   */
  def publishFlow(eventBridgeClient: EventBridgeAsyncClient): Flow[PutEventsRequest, PutEventsResponse, NotUsed] =
    akka.stream.alpakka.aws.eventbridge.scaladsl.EventBridgePublisher
      .publishFlow(EventBridgePublishSettings())(eventBridgeClient)
      .asJava

  /**
   * Creates a [[akka.stream.javadsl.Sink Sink]] to publish a message to an EventBridge.
   *
   * @param eventBridgeClient [[software.amazon.awssdk.services.eventbridge.EventBridgeAsyncClient EventBridgeAsyncClient]] client for publishing
   */
  def sink(eventBridgeClient: EventBridgeAsyncClient): Sink[PutEventsRequestEntry, CompletionStage[Done]] =
    flow(EventBridgePublishSettings(), eventBridgeClient)
      .toMat(Sink.ignore(), Keep.right[NotUsed, CompletionStage[Done]])

  /**
   * Creates a [[akka.stream.javadsl.Sink Sink]] to publish a message to an EventBridge.
   *
   * @param settings [[akka.stream.alpakka.aws.eventbridge.EventBridgePublishSettings]] settings for publishing
   * @param eventBridgeClient [[software.amazon.awssdk.services.eventbridge.EventBridgeAsyncClient EventBridgeAsyncClient]] client for publishing
   */
  def sink(settings: EventBridgePublishSettings,
           eventBridgeClient: EventBridgeAsyncClient): Sink[PutEventsRequestEntry, CompletionStage[Done]] =
    flow(settings, eventBridgeClient)
      .toMat(Sink.ignore(), Keep.right[NotUsed, CompletionStage[Done]])

  /**
   * Creates a [[akka.stream.javadsl.Sink Sink]] to publish messages to an EventBridge based on the message bus arn.
   *
   * @param settings [[akka.stream.alpakka.aws.eventbridge.EventBridgePublishSettings]] settings for publishing
   * @param eventBridgeClient [[software.amazon.awssdk.services.eventbridge.EventBridgeAsyncClient EventBridgeAsyncClient]] client for publishing
   */
  def publishSink(settings: EventBridgePublishSettings,
                  eventBridgeClient: EventBridgeAsyncClient): Sink[PutEventsRequest, CompletionStage[Done]] =
    publishFlow(settings, eventBridgeClient)
      .toMat(Sink.ignore(), Keep.right[NotUsed, CompletionStage[Done]])

  /**
   * Creates a [[akka.stream.javadsl.Sink Sink]] to publish messages to an EventBridge based on the message bus arn.
   *
   * @param eventBridgeClient [[software.amazon.awssdk.services.eventbridge.EventBridgeAsyncClient EventBridgeAsyncClient]] client for publishing
   */
  def publishSink(eventBridgeClient: EventBridgeAsyncClient): Sink[PutEventsRequest, CompletionStage[Done]] =
    publishFlow(EventBridgePublishSettings(), eventBridgeClient)
      .toMat(Sink.ignore(), Keep.right[NotUsed, CompletionStage[Done]])
}
