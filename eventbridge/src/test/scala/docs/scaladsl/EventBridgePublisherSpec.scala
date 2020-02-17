/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.scaladsl

import akka.Done
import akka.stream.alpakka.eventbridge.IntegrationTestContext
import akka.stream.alpakka.eventbridge.scaladsl.EventBridgePublisher
import akka.stream.alpakka.testkit.scaladsl.LogCapturing
import akka.stream.scaladsl.{Sink, Source}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import software.amazon.awssdk.services.eventbridge.model.{PutEventsRequest, PutEventsRequestEntry}

import scala.concurrent.Future
import scala.concurrent.duration._

class EventBridgePublisherSpec
    extends AnyFlatSpec
    with Matchers
    with ScalaFutures
    with IntegrationTestContext
    with LogCapturing {

  implicit val defaultPatience =
    PatienceConfig(timeout = 15.seconds, interval = 100.millis)

  "EventBridge Publisher sink" should "put string event" in {
    val published: Future[Done] =
      //#use-sink
      Source
        .single("message")
        .runWith(EventBridgePublisher.sink(eventBusName, source, detailType))

    //#use-sink
    published.futureValue should be(Done)
  }

  it should "put event request with default event bus" in {
    val published: Future[Done] =
      //#use-sink
      Source
        .single(
          PutEventsRequest
            .builder()
            .entries(
              PutEventsRequestEntry
                .builder()
                .source("source")
                .detailType("detail-type")
                .detail("event-detail")
                .build()
            )
            .build()
        )
        .runWith(EventBridgePublisher.putEventsSink())

    //#use-sink
    published.futureValue should be(Done)
  }

  it should "put event request with custom event bus" in {
    val published: Future[Done] =
      //#use-sink
      Source
        .single(
          PutEventsRequest
            .builder()
            .entries(
              PutEventsRequestEntry
                .builder()
                .eventBusName("event-bus")
                .source("source")
                .detailType("detail-type")
                .detail("event-detail")
                .build()
            )
            .build()
        )
        .runWith(EventBridgePublisher.putEventsSink())
    //#use-sink
    published.futureValue should be(Done)
  }

  "EventBridge Publisher flow" should "put string event" in {
    val published: Future[Done] =
      //#use-flow
      Source
        .single("message")
        .via(EventBridgePublisher.flow("event-bus", "source", "detail-type"))
        .runWith(Sink.foreach(resp => println(resp.entries())))

    //#use-flow
    published.futureValue should be(Done)
  }

  it should "put event request to default event bus" in {
    val published: Future[Done] =
      //#use-flow
      Source
        .single(
          PutEventsRequest
            .builder()
            .entries(
              PutEventsRequestEntry
                .builder()
                .source("source")
                .detailType("detail-type")
                .detail("event-detail")
                .build()
            )
            .build()
        )
        .via(EventBridgePublisher.putEventsFlow())
        .runWith(Sink.foreach(resp => println(resp.entries())))

    //#use-flow
    published.futureValue should be(Done)
  }

  it should "put event request with custom event bus" in {
    val published: Future[Done] =
      //#use-flow
      Source
        .single(
          PutEventsRequest
            .builder()
            .entries(
              PutEventsRequestEntry
                .builder()
                .eventBusName("event-bus")
                .source("source")
                .detailType("detail-type")
                .detail("event-detail")
                .build()
            )
            .build()
        )
        .via(EventBridgePublisher.putEventsFlow())
        .runWith(Sink.foreach(resp => println(resp.entries())))
    //#use-flow
    published.futureValue should be(Done)
  }

}
