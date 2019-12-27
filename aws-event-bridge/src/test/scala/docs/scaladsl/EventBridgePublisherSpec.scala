/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package docs.scaladsl

import akka.Done
import akka.stream.alpakka.aws.eventbridge.IntegrationTestContext
import akka.stream.alpakka.aws.eventbridge.scaladsl.EventBridgePublisher
import akka.stream.scaladsl.{Sink, Source}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{FlatSpec, Matchers}
import software.amazon.awssdk.services.eventbridge.model.{PutEventsRequest, PutEventsRequestEntry}

import scala.concurrent.Future
import scala.concurrent.duration._

class EventBridgePublisherSpec extends FlatSpec with Matchers with ScalaFutures with IntegrationTestContext {

  implicit val defaultPatience =
    PatienceConfig(timeout = 15.seconds, interval = 100.millis)

  "EventBridge Publisher sink" should "send PutEventsEntry message" in {
    val published: Future[Done] =
      //#use-sink
      Source
        .single(PutEventsRequestEntry.builder().detail("string").build())
        .runWith(EventBridgePublisher.sink())

    //#use-sink
    published.futureValue should be(Done)
  }

  it should "send put events request" in {
    val published: Future[Done] =
      //#use-sink
      Source
        .single(PutEventsRequest.builder().entries(PutEventsRequestEntry.builder().detail("string").build()).build())
        .runWith(EventBridgePublisher.publishSink())

    //#use-sink
    published.futureValue should be(Done)
  }

  "EventBridge put flow" should "send PutEventsRequestEntry message" in {
    val published: Future[Done] =
      //#use-flow
      Source
        .single(PutEventsRequestEntry.builder().detail("string").build())
        .via(EventBridgePublisher.flow())
        .runWith(Sink.foreach(res => println(res)))

    //#use-flow
    published.futureValue should be(Done)
  }

  it should "send publish request" in {
    val published: Future[Done] =
      //#use-flow
      Source
        .single(PutEventsRequest.builder().entries(PutEventsRequestEntry.builder().detail("string").build()).build())
        .via(EventBridgePublisher.publishFlow())
        .runWith(Sink.foreach(res => println(res)))

    //#use-flow
    published.futureValue should be(Done)
  }

}
