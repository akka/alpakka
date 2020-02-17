/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.eventbridge.scaladsl

import akka.stream.alpakka.eventbridge.EventBridgePublishSettings
import akka.stream.scaladsl.{Flow, Keep, Sink}
import akka.{Done, NotUsed}
import software.amazon.awssdk.services.eventbridge.EventBridgeAsyncClient
import software.amazon.awssdk.services.eventbridge.model.{PutEventsRequest, PutEventsRequestEntry, PutEventsResponse}

import scala.compat.java8.FutureConverters._
import scala.concurrent.Future

/**
 * Scala API
 * Amazon EventBridge publisher factory.
 */
object EventBridgePublisher {

  /**
   * creates a [[akka.stream.scaladsl.Flow Flow]] to publish event to a EventBridge EventBus using an [[software.amazon.awssdk.services.eventbridge.EventBridgeAsyncClient EventBridgeAsyncClient]]
   */
  def flow(eventBusName: String,
           source: String,
           detailType: String,
           settings: EventBridgePublishSettings = EventBridgePublishSettings())(
      implicit eventBridgeClient: EventBridgeAsyncClient
  ): Flow[String, PutEventsResponse, NotUsed] =
    Flow
      .fromFunction(
        (message: String) =>
          PutEventsRequest
            .builder()
            .entries(
              PutEventsRequestEntry
                .builder()
                .eventBusName(eventBusName)
                .source(source)
                .detailType(detailType)
                .detail(message)
                .build()
            )
            .build()
      )
      .via(putEventsFlow(settings))

  /**
   * creates a [[akka.stream.scaladsl.Flow Flow]] to publish event to EventBridge EventBus based on the event bus name using an [[software.amazon.awssdk.services.eventbridge.EventBridgeAsyncClient EventBridgeAsyncClient]]
   */
  def putEventsFlow(settings: EventBridgePublishSettings)(
      implicit eventBridgeClient: EventBridgeAsyncClient
  ): Flow[PutEventsRequest, PutEventsResponse, NotUsed] =
    Flow[PutEventsRequest]
      .mapAsyncUnordered(settings.concurrency)(eventBridgeClient.putEvents(_).toScala)

  /**
   * creates a [[akka.stream.scaladsl.Flow Flow]] to publish messages to EventBridge EventBus based on the event bus name using an [[software.amazon.awssdk.services.eventbridge.EventBridgeAsyncClient EventBridgeAsyncClient]]
   */
  def putEventsFlow()(
      implicit eventBridgeClient: EventBridgeAsyncClient
  ): Flow[PutEventsRequest, PutEventsResponse, NotUsed] =
    putEventsFlow(EventBridgePublishSettings())

  /**
   * creates a [[akka.stream.scaladsl.Sink Sink]] to publish messages to a EventBridge EventBus using an [[software.amazon.awssdk.services.eventbridge.EventBridgeAsyncClient EventBridgeAsyncClient]]
   */
  def sink(eventBus: String,
           source: String,
           detailType: String,
           settings: EventBridgePublishSettings = EventBridgePublishSettings())(
      implicit eventBridgeClient: EventBridgeAsyncClient
  ): Sink[String, Future[Done]] =
    flow(eventBus, source, detailType, settings).toMat(Sink.ignore)(Keep.right)

  /**
   * creates a [[akka.stream.scaladsl.Sink Sink]] to publish messages to a EventBridge EventBus using an [[software.amazon.awssdk.services.eventbridge.EventBridgeAsyncClient EventBridgeAsyncClient]]
   */
  def putEventsSink(settings: EventBridgePublishSettings = EventBridgePublishSettings())(
      implicit eventBridgeClient: EventBridgeAsyncClient
  ): Sink[PutEventsRequest, Future[Done]] =
    putEventsFlow(settings).toMat(Sink.ignore)(Keep.right)

  /**
   * creates a [[akka.stream.scaladsl.Sink Sink]] to publish messages to EventBridge EventBus based on the event bus name using an [[software.amazon.awssdk.services.eventbridge.EventBridgeAsyncClient EventBridgeAsyncClient]]
   */
  def putEventsSink()(implicit eventBridgeClient: EventBridgeAsyncClient): Sink[PutEventsRequest, Future[Done]] =
    putEventsFlow(EventBridgePublishSettings()).toMat(Sink.ignore)(Keep.right)
}
