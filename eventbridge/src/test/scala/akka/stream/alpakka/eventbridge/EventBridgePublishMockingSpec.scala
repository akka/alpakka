/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.eventbridge

import java.util.concurrent.CompletableFuture

import akka.stream.alpakka.eventbridge.scaladsl.EventBridgePublisher
import akka.stream.alpakka.testkit.scaladsl.LogCapturing
import akka.stream.scaladsl.{Keep, Sink}
import akka.stream.testkit.scaladsl.TestSource
import org.mockito.ArgumentMatchers.{any, eq => meq}
import org.mockito.Mockito._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.must.Matchers
import software.amazon.awssdk.services.eventbridge.model.{
  PutEventsRequest,
  PutEventsRequestEntry,
  PutEventsResponse,
  PutEventsResultEntry
}

import scala.concurrent.Await
import scala.concurrent.duration._

class EventBridgePublishMockingSpec extends AnyFlatSpec with DefaultTestContext with Matchers with LogCapturing {

  it should "put a single PutEventsRequest events to EventBridge" in {
    val putEventsRequest = PutEventsRequest
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
    val putEventsResponse =
      PutEventsResponse.builder().entries(PutEventsResultEntry.builder().eventId("event-id").build()).build()

    when(eventBridgeClient.putEvents(meq(putEventsRequest)))
      .thenReturn(CompletableFuture.completedFuture(putEventsResponse))

    val (probe, future) =
      TestSource.probe[PutEventsRequest].via(EventBridgePublisher.putEventsFlow()).toMat(Sink.seq)(Keep.both).run()

    probe
      .sendNext(
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
      .sendComplete()

    Await.result(future, 1.second) mustBe putEventsResponse :: Nil

    verify(eventBridgeClient, times(1)).putEvents(meq(putEventsRequest))
  }

  it should "put multiple PutEventsRequest events to EventBridge" in {
    val putEventsResponse =
      PutEventsResponse.builder().entries(PutEventsResultEntry.builder().eventId("event-id").build()).build()

    when(eventBridgeClient.putEvents(any[PutEventsRequest]()))
      .thenReturn(CompletableFuture.completedFuture(putEventsResponse))

    val (probe, future) =
      TestSource.probe[PutEventsRequest].via(EventBridgePublisher.putEventsFlow()).toMat(Sink.seq)(Keep.both).run()

    probe
      .sendNext(
        PutEventsRequest
          .builder()
          .entries(
            PutEventsRequestEntry
              .builder()
              .eventBusName("event-bus")
              .source("source")
              .detailType("detail-type")
              .detail("event-detail-1")
              .build()
          )
          .build()
      )
      .sendNext(
        PutEventsRequest
          .builder()
          .entries(
            PutEventsRequestEntry
              .builder()
              .eventBusName("event-bus")
              .source("source")
              .detailType("detail-type")
              .detail("event-detail-2")
              .build()
          )
          .build()
      )
      .sendNext(
        PutEventsRequest
          .builder()
          .entries(
            PutEventsRequestEntry
              .builder()
              .eventBusName("event-bus")
              .source("source")
              .detailType("detail-type")
              .detail("event-detail-3")
              .build()
          )
          .build()
      )
      .sendComplete()

    Await.result(future, 1.second) mustBe putEventsResponse :: putEventsResponse :: putEventsResponse :: Nil

    val expectedFirst = PutEventsRequest
      .builder()
      .entries(
        PutEventsRequestEntry
          .builder()
          .eventBusName("event-bus")
          .source("source")
          .detailType("detail-type")
          .detail("event-detail-1")
          .build()
      )
      .build()
    verify(eventBridgeClient, times(1)).putEvents(meq(expectedFirst))

    val expectedSecond = PutEventsRequest
      .builder()
      .entries(
        PutEventsRequestEntry
          .builder()
          .eventBusName("event-bus")
          .source("source")
          .detailType("detail-type")
          .detail("event-detail-2")
          .build()
      )
      .build()
    verify(eventBridgeClient, times(1)).putEvents(meq(expectedSecond))

    val expectedThird = PutEventsRequest
      .builder()
      .entries(
        PutEventsRequestEntry
          .builder()
          .eventBusName("event-bus")
          .source("source")
          .detailType("detail-type")
          .detail("event-detail-3")
          .build()
      )
      .build()
    verify(eventBridgeClient, times(1)).putEvents(meq(expectedThird))
  }

  it should "put multiple PutEventsRequest events to multiple EventBridge EventBuses" in {
    val putEventsResponse =
      PutEventsResponse.builder().entries(PutEventsResultEntry.builder().eventId("event-id").build()).build()

    when(eventBridgeClient.putEvents(any[PutEventsRequest]()))
      .thenReturn(CompletableFuture.completedFuture(putEventsResponse))

    val (probe, future) =
      TestSource.probe[PutEventsRequest].via(EventBridgePublisher.putEventsFlow()).toMat(Sink.seq)(Keep.both).run()

    probe
      .sendNext(
        PutEventsRequest
          .builder()
          .entries(
            PutEventsRequestEntry
              .builder()
              .eventBusName("event-bus-1")
              .source("source")
              .detailType("detail-type")
              .detail("event-detail-1")
              .build()
          )
          .build()
      )
      .sendNext(
        PutEventsRequest
          .builder()
          .entries(
            PutEventsRequestEntry
              .builder()
              .eventBusName("event-bus-2")
              .source("source")
              .detailType("detail-type")
              .detail("event-detail-2")
              .build()
          )
          .build()
      )
      .sendNext(
        PutEventsRequest
          .builder()
          .entries(
            PutEventsRequestEntry
              .builder()
              .eventBusName("event-bus-3")
              .source("source")
              .detailType("detail-type")
              .detail("event-detail-3")
              .build()
          )
          .build()
      )
      .sendComplete()

    Await.result(future, 1.second) mustBe putEventsResponse :: putEventsResponse :: putEventsResponse :: Nil

    val expectedFirst = PutEventsRequest
      .builder()
      .entries(
        PutEventsRequestEntry
          .builder()
          .eventBusName("event-bus-1")
          .source("source")
          .detailType("detail-type")
          .detail("event-detail-1")
          .build()
      )
      .build()
    verify(eventBridgeClient, times(1)).putEvents(meq(expectedFirst))

    val expectedSecond = PutEventsRequest
      .builder()
      .entries(
        PutEventsRequestEntry
          .builder()
          .eventBusName("event-bus-2")
          .source("source")
          .detailType("detail-type")
          .detail("event-detail-2")
          .build()
      )
      .build()
    verify(eventBridgeClient, times(1)).putEvents(meq(expectedSecond))

    val expectedThird = PutEventsRequest
      .builder()
      .entries(
        PutEventsRequestEntry
          .builder()
          .eventBusName("event-bus-3")
          .source("source")
          .detailType("detail-type")
          .detail("event-detail-3")
          .build()
      )
      .build()
    verify(eventBridgeClient, times(1)).putEvents(meq(expectedThird))
  }

  it should "put a single string events to EventBridge" in {
    val putEventsRequest = PutEventsRequest
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
    val putEventsResponse =
      PutEventsResponse.builder().entries(PutEventsResultEntry.builder().eventId("event-id").build()).build()

    when(eventBridgeClient.putEvents(meq(putEventsRequest)))
      .thenReturn(CompletableFuture.completedFuture(putEventsResponse))

    val (probe, future) = TestSource
      .probe[String]
      .via(EventBridgePublisher.flow("event-bus", "source", "detail-type"))
      .toMat(Sink.seq)(Keep.both)
      .run()
    probe.sendNext("event-detail").sendComplete()

    Await.result(future, 1.second) mustBe putEventsResponse :: Nil
    verify(eventBridgeClient, times(1)).putEvents(meq(putEventsRequest))
  }

  it should "put multiple string events to EventBridge" in {
    val putEventsResponse =
      PutEventsResponse.builder().entries(PutEventsResultEntry.builder().eventId("event-id").build()).build()

    when(eventBridgeClient.putEvents(any[PutEventsRequest]()))
      .thenReturn(CompletableFuture.completedFuture(putEventsResponse))

    val (probe, future) = TestSource
      .probe[String]
      .via(EventBridgePublisher.flow("event-bus", "source", "detail-type"))
      .toMat(Sink.seq)(Keep.both)
      .run()
    probe.sendNext("event-detail-1").sendNext("event-detail-2").sendNext("event-detail-3").sendComplete()

    Await.result(future, 1.second) mustBe putEventsResponse :: putEventsResponse :: putEventsResponse :: Nil

    val expectedFirst = PutEventsRequest
      .builder()
      .entries(
        PutEventsRequestEntry
          .builder()
          .eventBusName("event-bus")
          .source("source")
          .detailType("detail-type")
          .detail("event-detail-1")
          .build()
      )
      .build()
    verify(eventBridgeClient, times(1)).putEvents(meq(expectedFirst))

    val expectedSecond = PutEventsRequest
      .builder()
      .entries(
        PutEventsRequestEntry
          .builder()
          .eventBusName("event-bus")
          .source("source")
          .detailType("detail-type")
          .detail("event-detail-2")
          .build()
      )
      .build()
    verify(eventBridgeClient, times(1)).putEvents(meq(expectedSecond))

    val expectedThird = PutEventsRequest
      .builder()
      .entries(
        PutEventsRequestEntry
          .builder()
          .eventBusName("event-bus")
          .source("source")
          .detailType("detail-type")
          .detail("event-detail-3")
          .build()
      )
      .build()
    verify(eventBridgeClient, times(1)).putEvents(meq(expectedThird))
  }

  it should "fail stage if AmazonEventBridgeAsyncClient request failed" in {
    val putEventsRequest = PutEventsRequest
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

    val promise = new CompletableFuture[PutEventsResponse]()
    promise.completeExceptionally(new RuntimeException("publish error"))

    when(eventBridgeClient.putEvents(meq(putEventsRequest))).thenReturn(promise)

    val (probe, future) = TestSource
      .probe[String]
      .via(EventBridgePublisher.flow("event-bus", "source", "detail-type"))
      .toMat(Sink.seq)(Keep.both)
      .run()
    probe.sendNext("event-detail").sendComplete()

    a[RuntimeException] should be thrownBy {
      Await.result(future, 1.second)
    }

    verify(eventBridgeClient, times(1)).putEvents(meq(putEventsRequest))
  }

  it should "fail stage if upstream failure occurs" in {
    case class MyCustomException(message: String) extends Exception(message)

    val (probe, future) = TestSource
      .probe[String]
      .via(EventBridgePublisher.flow("event-bus", "source", "detail-type"))
      .toMat(Sink.seq)(Keep.both)
      .run()
    probe.sendError(MyCustomException("upstream failure"))

    a[MyCustomException] should be thrownBy {
      Await.result(future, 1.second)
    }

    verify(eventBridgeClient, never()).putEvents(any[PutEventsRequest]())
  }

}
