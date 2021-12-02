/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.scaladsl

import akka.Done
import akka.stream.alpakka.aws.eventbridge.IntegrationTestContext
import akka.stream.alpakka.aws.eventbridge.scaladsl.EventBridgePublisher
import akka.stream.scaladsl.{Sink, Source}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.flatspec._
import org.scalatest.matchers.should.Matchers
import software.amazon.awssdk.services.eventbridge.model.{PutEventsRequest, PutEventsRequestEntry}

import scala.concurrent.Future
import scala.concurrent.duration._

class EventBridgePublisherSpec extends AnyFlatSpec with Matchers with ScalaFutures with IntegrationTestContext {

  implicit val defaultPatience =
    PatienceConfig(timeout = 15.seconds, interval = 100.millis)

  "EventBridge Publisher sink" should "send PutEventsEntry message" in {
    val published: Future[Done] =
      // #run-events-entry
      Source
        .single(PutEventsRequestEntry.builder().detail("string").build())
        .runWith(EventBridgePublisher.sink())

    // #run-events-entry
    published.futureValue should be(Done)
  }

  it should "send put events request" in {
    val published: Future[Done] =
      // #run-events-request
      Source
        .single(PutEventsRequest.builder().entries(PutEventsRequestEntry.builder().detail("string").build()).build())
        .runWith(EventBridgePublisher.publishSink())

    // #run-events-request
    published.futureValue should be(Done)
  }

  "EventBridge put flow" should "send PutEventsRequestEntry message" in {
    val published: Future[Done] =
      // #flow-events-entry
      Source
        .single(PutEventsRequestEntry.builder().detail("string").build())
        .via(EventBridgePublisher.flow())
        .runWith(Sink.foreach(res => println(res)))
    // #flow-events-entry
    published.futureValue should be(Done)
  }

  it should "send publish request" in {
    val published: Future[Done] =
      // #flow-events-request
      Source
        .single(PutEventsRequest.builder().entries(PutEventsRequestEntry.builder().detail("string").build()).build())
        .via(EventBridgePublisher.publishFlow())
        .runWith(Sink.foreach(res => println(res)))
    // #flow-events-request
    published.futureValue should be(Done)
  }

}
