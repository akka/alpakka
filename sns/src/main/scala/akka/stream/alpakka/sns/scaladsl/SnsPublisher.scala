/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.sns.scaladsl

import akka.stream.alpakka.sns.SnsPublishFlowStage
import akka.stream.scaladsl.{Flow, Keep, Sink}
import akka.{Done, NotUsed}
import com.amazonaws.services.sns.AmazonSNSAsync
import com.amazonaws.services.sns.model.PublishResult

import scala.concurrent.Future

object SnsPublisher {

  /**
   * Scala API: creates a [[Sink]] to publish messages to a SNS topic using an [[AmazonSNSAsync]]
   */
  def flow(topicArn: String)(implicit snsClient: AmazonSNSAsync): Flow[String, PublishResult, NotUsed] =
    Flow.fromGraph(new SnsPublishFlowStage(topicArn, snsClient))

  /**
   * Scala API: creates a [[Sink]] to publish messages to a SNS topic using an [[AmazonSNSAsync]]
   */
  def sink(topicArn: String)(implicit snsClient: AmazonSNSAsync): Sink[String, Future[Done]] =
    flow(topicArn).toMat(Sink.ignore)(Keep.right)

}
