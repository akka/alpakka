/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.sns.scaladsl
import akka.stream.alpakka.sns.SnsPublishSettings
import akka.stream.scaladsl.{Flow, Keep, Sink}
import akka.{Done, NotUsed}
import software.amazon.awssdk.services.sns.SnsAsyncClient
import software.amazon.awssdk.services.sns.model.{PublishRequest, PublishResponse}

import scala.concurrent.Future

import scala.compat.java8.FutureConverters._

/**
 * Scala API
 * Amazon SNS publisher factory.
 */
object SnsPublisher {

  /**
   * creates a [[akka.stream.scaladsl.Flow Flow]] to publish messages to a SNS topic using an [[software.amazon.awssdk.services.sns.SnsAsyncClient SnsAsyncClient]]
   */
  def flow(topicArn: String, settings: SnsPublishSettings = SnsPublishSettings())(implicit
      snsClient: SnsAsyncClient
  ): Flow[String, PublishResponse, NotUsed] =
    Flow
      .fromFunction((message: String) => PublishRequest.builder().message(message).topicArn(topicArn).build())
      .via(publishFlow(settings))

  /**
   * creates a [[akka.stream.scaladsl.Flow Flow]] to publish messages to a SNS topic using an [[software.amazon.awssdk.services.sns.SnsAsyncClient SnsAsyncClient]]
   */
  def publishFlow(topicArn: String, settings: SnsPublishSettings = SnsPublishSettings())(implicit
      snsClient: SnsAsyncClient
  ): Flow[PublishRequest, PublishResponse, NotUsed] =
    Flow
      .fromFunction((request: PublishRequest) => request.toBuilder.topicArn(topicArn).build())
      .via(publishFlow(settings))

  /**
   * creates a [[akka.stream.scaladsl.Flow Flow]] to publish messages to SNS topics based on the message topic arn using an [[software.amazon.awssdk.services.sns.SnsAsyncClient SnsAsyncClient]]
   */
  def publishFlow(
      settings: SnsPublishSettings
  )(implicit snsClient: SnsAsyncClient): Flow[PublishRequest, PublishResponse, NotUsed] = {
    require(snsClient != null, "The `SnsAsyncClient` passed in may not be null.")
    Flow[PublishRequest]
      .mapAsyncUnordered(settings.concurrency)(snsClient.publish(_).toScala)
  }

  /**
   * creates a [[akka.stream.scaladsl.Flow Flow]] to publish messages to SNS topics based on the message topic arn using an [[software.amazon.awssdk.services.sns.SnsAsyncClient SnsAsyncClient]]
   */
  def publishFlow()(implicit snsClient: SnsAsyncClient): Flow[PublishRequest, PublishResponse, NotUsed] =
    publishFlow(SnsPublishSettings())

  /**
   * creates a [[akka.stream.scaladsl.Sink Sink]] to publish messages to a SNS topic using an [[software.amazon.awssdk.services.sns.SnsAsyncClient SnsAsyncClient]]
   */
  def sink(topicArn: String, settings: SnsPublishSettings = SnsPublishSettings())(implicit
      snsClient: SnsAsyncClient
  ): Sink[String, Future[Done]] =
    flow(topicArn, settings).toMat(Sink.ignore)(Keep.right)

  /**
   * creates a [[akka.stream.scaladsl.Sink Sink]] to publish messages to a SNS topic using an [[software.amazon.awssdk.services.sns.SnsAsyncClient SnsAsyncClient]]
   */
  def publishSink(topicArn: String, settings: SnsPublishSettings = SnsPublishSettings())(implicit
      snsClient: SnsAsyncClient
  ): Sink[PublishRequest, Future[Done]] =
    publishFlow(topicArn, settings).toMat(Sink.ignore)(Keep.right)

  /**
   * creates a [[akka.stream.scaladsl.Sink Sink]] to publish messages to SNS topics based on the message topic arn using an [[software.amazon.awssdk.services.sns.SnsAsyncClient SnsAsyncClient]]
   */
  def publishSink(
      settings: SnsPublishSettings
  )(implicit snsClient: SnsAsyncClient): Sink[PublishRequest, Future[Done]] =
    publishFlow(settings).toMat(Sink.ignore)(Keep.right)

  /**
   * creates a [[akka.stream.scaladsl.Sink Sink]] to publish messages to SNS topics based on the message topic arn using an [[software.amazon.awssdk.services.sns.SnsAsyncClient SnsAsyncClient]]
   */
  def publishSink()(implicit snsClient: SnsAsyncClient): Sink[PublishRequest, Future[Done]] =
    publishFlow(SnsPublishSettings()).toMat(Sink.ignore)(Keep.right)
}
