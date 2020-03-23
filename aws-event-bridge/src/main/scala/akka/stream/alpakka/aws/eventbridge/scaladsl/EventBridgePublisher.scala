/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.aws.eventbridge.scaladsl

import akka.stream.alpakka.aws.eventbridge.EventBridgePublishSettings
import akka.stream.scaladsl.{Flow, Keep, Sink}
import akka.{Done, NotUsed}
import software.amazon.awssdk.services.eventbridge.EventBridgeAsyncClient
import software.amazon.awssdk.services.eventbridge.model._

import scala.concurrent.Future
import scala.compat.java8.FutureConverters._

/**
 * Scala API
 * Amazon Event Bridge publisher factory.
 */
object EventBridgePublisher {

  /**
   * Creates a [[akka.stream.scaladsl.Flow Flow]] to publish a message to an EventBridge.
   *
   * @param settings [[akka.stream.alpakka.aws.eventbridge.EventBridgePublishSettings]] settings for publishing
   * @param eventBridgeClient [[software.amazon.awssdk.services.eventbridge.EventBridgeAsyncClient EventBridgeAsyncClient]] client for publishing
   */
  def flow(settings: EventBridgePublishSettings = EventBridgePublishSettings())(
      implicit eventBridgeClient: EventBridgeAsyncClient
  ): Flow[PutEventsRequestEntry, PutEventsResponse, NotUsed] =
    Flow
      .fromFunction((message: PutEventsRequestEntry) => PutEventsRequest.builder().entries(message).build())
      .via(publishFlow(settings))

  /**
   * Creates a [[akka.stream.scaladsl.Flow Flow]] to publish a message to an EventBridge.
   *
   * @param settings [[akka.stream.alpakka.aws.eventbridge.EventBridgePublishSettings]] settings for publishing
   * @param eventBridgeClient [[software.amazon.awssdk.services.eventbridge.EventBridgeAsyncClient EventBridgeAsyncClient]] client for publishing
   */
  def flowSeq(settings: EventBridgePublishSettings = EventBridgePublishSettings())(
      implicit eventBridgeClient: EventBridgeAsyncClient
  ): Flow[Seq[PutEventsRequestEntry], PutEventsResponse, NotUsed] =
    Flow
      .fromFunction((messages: Seq[PutEventsRequestEntry]) => PutEventsRequest.builder().entries(messages: _*).build())
      .via(publishFlow(settings))

  /**
   * Creates a [[akka.stream.scaladsl.Flow Flow]] to publish messages to an EventBridge based on the message bus arn.
   *
   * @param settings [[akka.stream.alpakka.aws.eventbridge.EventBridgePublishSettings]] settings for publishing
   * @param eventBridgeClient [[software.amazon.awssdk.services.eventbridge.EventBridgeAsyncClient EventBridgeAsyncClient]] client for publishing
   */
  def publishFlow(
      settings: EventBridgePublishSettings
  )(implicit eventBridgeClient: EventBridgeAsyncClient): Flow[PutEventsRequest, PutEventsResponse, NotUsed] =
    Flow[PutEventsRequest]
      .mapAsyncUnordered(settings.concurrency)(eventBridgeClient.putEvents(_).toScala)

  /**
   * Creates a [[akka.stream.scaladsl.Flow Flow]] to publish messages to an EventBridge based on the message bus arn.
   *
   * @param eventBridgeClient [[software.amazon.awssdk.services.eventbridge.EventBridgeAsyncClient EventBridgeAsyncClient]] client for publishing
   */
  def publishFlow()(
      implicit eventBridgeClient: EventBridgeAsyncClient
  ): Flow[PutEventsRequest, PutEventsResponse, NotUsed] =
    publishFlow(EventBridgePublishSettings())

  /**
   * Creates a [[akka.stream.scaladsl.Sink Sink]] to publish messages to an EventBridge.
   *
   * @param settings [[akka.stream.alpakka.aws.eventbridge.EventBridgePublishSettings]] settings for publishing
   * @param eventBridgeClient [[software.amazon.awssdk.services.eventbridge.EventBridgeAsyncClient EventBridgeAsyncClient]] client for publishing
   */
  def sink(settings: EventBridgePublishSettings = EventBridgePublishSettings())(
      implicit eventBridgeClient: EventBridgeAsyncClient
  ): Sink[PutEventsRequestEntry, Future[Done]] =
    flow(settings).toMat(Sink.ignore)(Keep.right)

  /**
   * Creates a [[akka.stream.scaladsl.Sink Sink]] to publish messages to an EventBridge based on the message bus arn.
   *
   * @param settings [[akka.stream.alpakka.aws.eventbridge.EventBridgePublishSettings]] settings for publishing
   * @param eventBridgeClient [[software.amazon.awssdk.services.eventbridge.EventBridgeAsyncClient EventBridgeAsyncClient]] client for publishing
   */
  def publishSink(
      settings: EventBridgePublishSettings = EventBridgePublishSettings()
  )(implicit eventBridgeClient: EventBridgeAsyncClient): Sink[PutEventsRequest, Future[Done]] =
    publishFlow(settings).toMat(Sink.ignore)(Keep.right)

  /**
   * Creates a [[akka.stream.scaladsl.Sink Sink]] to publish messages to an EventBridge based on the message bus arn.
   *
   * @param eventBridgeClient [[software.amazon.awssdk.services.eventbridge.EventBridgeAsyncClient EventBridgeAsyncClient]] client for publishing
   */
  def publishSink()(implicit eventBridgeClient: EventBridgeAsyncClient): Sink[PutEventsRequest, Future[Done]] =
    publishFlow(EventBridgePublishSettings()).toMat(Sink.ignore)(Keep.right)
}
