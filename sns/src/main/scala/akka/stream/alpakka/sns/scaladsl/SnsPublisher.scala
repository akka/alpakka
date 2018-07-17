/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.sns.scaladsl

import akka.stream.alpakka.sns.SnsPublishFlowStage
import akka.stream.scaladsl.{Flow, Keep, Sink}
import akka.{Done, NotUsed}
import com.amazonaws.services.sns.AmazonSNSAsync
import com.amazonaws.services.sns.model.{PublishRequest, PublishResult}

import scala.concurrent.Future

object SnsPublisher {

  /**
   * Scala API: creates a [[akka.stream.scaladsl.Flow Flow]] to publish messages to a SNS topic using an [[com.amazonaws.services.sns.AmazonSNSAsync AmazonSNSAsync]]
   */
  def flow(topicArn: String)(implicit snsClient: AmazonSNSAsync): Flow[String, PublishResult, NotUsed] =
    Flow
      .fromFunction((message: String) => new PublishRequest(topicArn, message))
      .via(publishFlow(topicArn))

  /**
   * Scala API: creates a [[akka.stream.scaladsl.Flow Flow]] to publish messages to a SNS topic using an [[com.amazonaws.services.sns.AmazonSNSAsync AmazonSNSAsync]]
   */
  def publishFlow(topicArn: String)(implicit snsClient: AmazonSNSAsync): Flow[PublishRequest, PublishResult, NotUsed] =
    Flow.fromGraph(new SnsPublishFlowStage(topicArn, snsClient))

  /**
   * Scala API: creates a [[akka.stream.scaladsl.Sink Sink]] to publish messages to a SNS topic using an [[com.amazonaws.services.sns.AmazonSNSAsync AmazonSNSAsync]]
   */
  def sink(topicArn: String)(implicit snsClient: AmazonSNSAsync): Sink[String, Future[Done]] =
    flow(topicArn).toMat(Sink.ignore)(Keep.right)

  /**
   * Scala API: creates a [[akka.stream.scaladsl.Sink Sink]] to publish messages to a SNS topic using an [[com.amazonaws.services.sns.AmazonSNSAsync AmazonSNSAsync]]
   */
  def publishSink(topicArn: String)(implicit snsClient: AmazonSNSAsync): Sink[PublishRequest, Future[Done]] =
    publishFlow(topicArn).toMat(Sink.ignore)(Keep.right)
}
