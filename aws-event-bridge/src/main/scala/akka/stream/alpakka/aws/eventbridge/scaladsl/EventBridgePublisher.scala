/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
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
   * creates a [[akka.stream.scaladsl.Flow Flow]] to publish messages to a Event Bridge using an [[software.amazon.awssdk.services.eventbridge.EventBridgeAsyncClient EventBridgeAsyncClient]]
   */
  def flow(settings: EventBridgePublishSettings = EventBridgePublishSettings())(
      implicit eventBridgeClient: EventBridgeAsyncClient
  ): Flow[PutEventsRequestEntry, PutEventsResponse, NotUsed] =
    Flow
      .fromFunction((message: PutEventsRequestEntry) => PutEventsRequest.builder().entries(message).build())
      .via(publishFlow(settings))

  /**
   * creates a [[akka.stream.scaladsl.Flow Flow]] to publish messages to a Event Bridge using an [[software.amazon.awssdk.services.eventbridge.EventBridgeAsyncClient EventBridgeAsyncClient]]
   */
  def flowSeq(settings: EventBridgePublishSettings = EventBridgePublishSettings())(
      implicit eventBridgeClient: EventBridgeAsyncClient
  ): Flow[Seq[PutEventsRequestEntry], PutEventsResponse, NotUsed] =
    Flow
      .fromFunction((messages: Seq[PutEventsRequestEntry]) => PutEventsRequest.builder().entries(messages: _*).build())
      .via(publishFlow(settings))

  /*
  PutEventsRequestEntry.builder()

    .time()
    .source()
    .resources()
    .detail
    .detailType
    .eventBusName()
   */

  /**
   * creates a [[akka.stream.scaladsl.Flow Flow]] to publish messages to SNS topics based on the message topic arn using an [[software.amazon.awssdk.services.eventbridge.EventBridgeAsyncClient EventBridgeAsyncClient]]
   */
  def publishFlow(
      settings: EventBridgePublishSettings
  )(implicit eventBridgeClient: EventBridgeAsyncClient): Flow[PutEventsRequest, PutEventsResponse, NotUsed] =
    Flow[PutEventsRequest]
      .mapAsyncUnordered(settings.concurrency)(eventBridgeClient.putEvents(_).toScala)

  /**
   * creates a [[akka.stream.scaladsl.Flow Flow]] to publish messages to SNS topics based on the message topic arn using an [[software.amazon.awssdk.services.eventbridge.EventBridgeAsyncClient EventBridgeAsyncClient]]
   */
  def publishFlow()(
      implicit eventBridgeClient: EventBridgeAsyncClient
  ): Flow[PutEventsRequest, PutEventsResponse, NotUsed] =
    publishFlow(EventBridgePublishSettings())

  /**
   * creates a [[akka.stream.scaladsl.Sink Sink]] to publish messages to a SNS topic using an [[software.amazon.awssdk.services.eventbridge.EventBridgeAsyncClient EventBridgeAsyncClient]]
   */
  def sink(settings: EventBridgePublishSettings = EventBridgePublishSettings())(
      implicit eventBridgeClient: EventBridgeAsyncClient
  ): Sink[PutEventsRequestEntry, Future[Done]] =
    flow(settings).toMat(Sink.ignore)(Keep.right)

  /**
   * creates a [[akka.stream.scaladsl.Sink Sink]] to publish messages to SNS topics based on the message topic arn using an [[software.amazon.awssdk.services.eventbridge.EventBridgeAsyncClient EventBridgeAsyncClient]]
   */
  def publishSink(
      settings: EventBridgePublishSettings = EventBridgePublishSettings()
  )(implicit eventBridgeClient: EventBridgeAsyncClient): Sink[PutEventsRequest, Future[Done]] =
    publishFlow(settings).toMat(Sink.ignore)(Keep.right)

  /**
   * creates a [[akka.stream.scaladsl.Sink Sink]] to publish messages to SNS topics based on the message topic arn using an [[software.amazon.awssdk.services.eventbridge.EventBridgeAsyncClient EventBridgeAsyncClient]]
   */
  def publishSink()(implicit eventBridgeClient: EventBridgeAsyncClient): Sink[PutEventsRequest, Future[Done]] =
    publishFlow(EventBridgePublishSettings()).toMat(Sink.ignore)(Keep.right)
}
