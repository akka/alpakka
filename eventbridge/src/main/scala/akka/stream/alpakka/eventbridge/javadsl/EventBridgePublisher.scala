/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.eventbridge.javadsl

import java.util.concurrent.CompletionStage

import akka.stream.alpakka.eventbridge.EventBridgePublishSettings
import akka.stream.javadsl.{Flow, Keep, Sink}
import akka.{Done, NotUsed}
import software.amazon.awssdk.services.eventbridge.EventBridgeAsyncClient
import software.amazon.awssdk.services.eventbridge.model.{PutEventsRequest, PutEventsResponse}

/**
 * Scala API
 * Amazon EventBridge publisher factory.
 */
object EventBridgePublisher {

  /**
   * creates a [[akka.stream.scaladsl.Flow Flow]] to publish event to a EventBridge EventBus using an [[software.amazon.awssdk.services.eventbridge.EventBridgeAsyncClient EventBridgeAsyncClient]]
   */
  def createFlow(eventBusName: String,
                 source: String,
                 detailType: String,
                 settings: EventBridgePublishSettings,
                 eventBridgeClient: EventBridgeAsyncClient): Flow[String, PutEventsResponse, NotUsed] =
    akka.stream.alpakka.eventbridge.scaladsl.EventBridgePublisher
      .flow(eventBusName, source, detailType, settings)(eventBridgeClient)
      .asJava

  /**
   * creates a [[akka.stream.scaladsl.Flow Flow]] to publish event to a EventBridge EventBus using an [[software.amazon.awssdk.services.eventbridge.EventBridgeAsyncClient EventBridgeAsyncClient]]
   */
  def createFlow(eventBusName: String,
                 source: String,
                 detailType: String,
                 eventBridgeClient: EventBridgeAsyncClient): Flow[String, PutEventsResponse, NotUsed] =
    akka.stream.alpakka.eventbridge.scaladsl.EventBridgePublisher
      .flow(eventBusName, source, detailType, EventBridgePublishSettings())(eventBridgeClient)
      .asJava

  /**
   * creates a [[akka.stream.scaladsl.Flow Flow]] to publish event to EventBridge EventBus based on the event bus name using an [[software.amazon.awssdk.services.eventbridge.EventBridgeAsyncClient EventBridgeAsyncClient]]
   */
  def createPutEventsFlow(
      settings: EventBridgePublishSettings,
      eventBridgeClient: EventBridgeAsyncClient
  ): Flow[PutEventsRequest, PutEventsResponse, NotUsed] =
    akka.stream.alpakka.eventbridge.scaladsl.EventBridgePublisher.putEventsFlow(settings)(eventBridgeClient).asJava

  /**
   * creates a [[akka.stream.scaladsl.Flow Flow]] to publish messages to EventBridge EventBus based on the event bus name using an [[software.amazon.awssdk.services.eventbridge.EventBridgeAsyncClient EventBridgeAsyncClient]]
   */
  def createPutEventsFlow(
      eventBridgeClient: EventBridgeAsyncClient
  ): Flow[PutEventsRequest, PutEventsResponse, NotUsed] =
    akka.stream.alpakka.eventbridge.scaladsl.EventBridgePublisher
      .putEventsFlow(EventBridgePublishSettings())(eventBridgeClient)
      .asJava

  /**
   * creates a [[akka.stream.scaladsl.Sink Sink]] to publish messages to a EventBridge EventBus using an [[software.amazon.awssdk.services.eventbridge.EventBridgeAsyncClient EventBridgeAsyncClient]]
   */
  def createSink(eventBus: String,
                 source: String,
                 detailType: String,
                 settings: EventBridgePublishSettings,
                 eventBridgeClient: EventBridgeAsyncClient): Sink[String, CompletionStage[Done]] =
    createFlow(eventBus, source, detailType, settings, eventBridgeClient)
      .toMat(Sink.ignore(), Keep.right[NotUsed, CompletionStage[Done]])

  /**
   * creates a [[akka.stream.scaladsl.Sink Sink]] to publish messages to a EventBridge EventBus using an [[software.amazon.awssdk.services.eventbridge.EventBridgeAsyncClient EventBridgeAsyncClient]]
   */
  def createSink(eventBus: String,
                 source: String,
                 detailType: String,
                 eventBridgeClient: EventBridgeAsyncClient): Sink[String, CompletionStage[Done]] =
    createFlow(eventBus, source, detailType, EventBridgePublishSettings(), eventBridgeClient)
      .toMat(Sink.ignore(), Keep.right[NotUsed, CompletionStage[Done]])

  /**
   * creates a [[akka.stream.scaladsl.Sink Sink]] to publish messages to a EventBridge EventBus using an [[software.amazon.awssdk.services.eventbridge.EventBridgeAsyncClient EventBridgeAsyncClient]]
   */
  def createPutEventsSink(settings: EventBridgePublishSettings,
                          eventBridgeClient: EventBridgeAsyncClient): Sink[PutEventsRequest, CompletionStage[Done]] =
    createPutEventsFlow(EventBridgePublishSettings(), eventBridgeClient)
      .toMat(Sink.ignore(), Keep.right[NotUsed, CompletionStage[Done]])

  /**
   * creates a [[akka.stream.scaladsl.Sink Sink]] to publish messages to a EventBridge EventBus using an [[software.amazon.awssdk.services.eventbridge.EventBridgeAsyncClient EventBridgeAsyncClient]]
   */
  def createPutEventsSink(eventBridgeClient: EventBridgeAsyncClient): Sink[PutEventsRequest, CompletionStage[Done]] =
    createPutEventsFlow(EventBridgePublishSettings(), eventBridgeClient)
      .toMat(Sink.ignore(), Keep.right[NotUsed, CompletionStage[Done]])
}
