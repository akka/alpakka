/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package docs.scaladsl

import akka.Done
import akka.stream.alpakka.sns.IntegrationTestContext
import akka.stream.alpakka.sns.scaladsl.SnsPublisher
import akka.stream.scaladsl.{Sink, Source}
import com.amazonaws.services.sns.model.PublishRequest
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{FlatSpec, Matchers}

import scala.concurrent.Future
import scala.concurrent.duration._

class SnsPublisherSpec extends FlatSpec with Matchers with ScalaFutures with IntegrationTestContext {

  implicit val defaultPatience =
    PatienceConfig(timeout = 15.seconds, interval = 100.millis)

  "SNS Publisher sink" should "send string message" in {
    val published: Future[Done] =
      //#use-sink
      Source
        .single("message")
        .runWith(SnsPublisher.sink(topicArn))

    //#use-sink
    published.futureValue should be(Done)
  }

  it should "send publish request" in {
    val published: Future[Done] =
      //#use-sink
      Source
        .single(new PublishRequest().withMessage("message"))
        .runWith(SnsPublisher.publishSink(topicArn))

    //#use-sink
    published.futureValue should be(Done)
  }

  it should "send publish request with dynamic arn" in {
    val published: Future[Done] =
      //#use-sink
      Source
        .single(new PublishRequest().withMessage("message").withTopicArn(topicArn))
        .runWith(SnsPublisher.publishSink())
    //#use-sink
    published.futureValue should be(Done)
  }

  "SNS Publisher flow" should "send string message" in {
    val published: Future[Done] =
      //#use-flow
      Source
        .single("message")
        .via(SnsPublisher.flow(topicArn))
        .runWith(Sink.foreach(res => println(res.getMessageId)))

    //#use-flow
    published.futureValue should be(Done)
  }

  it should "send publish request" in {
    val published: Future[Done] =
      //#use-flow
      Source
        .single(new PublishRequest().withMessage("message"))
        .via(SnsPublisher.publishFlow(topicArn))
        .runWith(Sink.foreach(res => println(res.getMessageId)))

    //#use-flow
    published.futureValue should be(Done)
  }

  it should "send publish request with dynamic topic" in {
    val published: Future[Done] =
      //#use-flow
      Source
        .single(new PublishRequest().withMessage("message").withTopicArn(topicArn))
        .via(SnsPublisher.publishFlow())
        .runWith(Sink.foreach(res => println(res.getMessageId)))
    //#use-flow
    published.futureValue should be(Done)
  }

}
