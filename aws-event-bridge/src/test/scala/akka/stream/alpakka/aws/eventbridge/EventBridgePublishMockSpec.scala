/*
 * Copyright (C) 2016-2020 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.aws.eventbridge

import java.util.concurrent.CompletableFuture

import akka.stream.alpakka.aws.eventbridge.scaladsl.EventBridgePublisher
import akka.stream.scaladsl.{Keep, Sink}
import akka.stream.testkit.scaladsl.TestSource
import org.mockito.ArgumentMatchers.{any, eq => meq}
import org.mockito.Mockito._
import org.scalatest._
import software.amazon.awssdk.services.eventbridge.model._

import scala.concurrent.Await
import scala.concurrent.duration._

class EventBridgePublishMockSpec extends FlatSpec with DefaultTestContext with MustMatchers {

  private def entryDetail(detail: String): PutEventsRequestEntry =
    PutEventsRequestEntry.builder().detail(detail).build()

  private def requestDetail(detail: String): PutEventsRequest =
    PutEventsRequest.builder().entries(entryDetail(detail)).build()

  private def resultResponse(eventId: String): PutEventsResponse = {
    val putResultEntry = PutEventsResultEntry.builder().eventId(eventId).build()
    PutEventsResponse.builder().entries(putResultEntry).build()
  }

  it should "publish a single PublishRequest message to eventBridge" in {
    val putRequestEntry = PutEventsRequestEntry.builder().detail("event-bridge-message").build()
    val putRequest = requestDetail("event-bridge-message")
    val putResult = resultResponse("event-id")

    when(eventBridgeClient.putEvents(putRequest)).thenReturn(CompletableFuture.completedFuture(putResult))

    val (probe, future) =
      TestSource.probe[PutEventsRequest].via(EventBridgePublisher.publishFlow()).toMat(Sink.seq)(Keep.both).run()

    probe.sendNext(PutEventsRequest.builder().entries(putRequestEntry).build()).sendComplete()

    Await.result(future, 1.second) mustBe putResult :: Nil

    verify(eventBridgeClient, times(1)).putEvents(meq(putRequest))
  }

  it should "publish multiple PutEventsRequest messages to eventbridge" in {
    val putResultEntry = PutEventsResultEntry.builder().eventId("event-id").build()
    val putResult = PutEventsResponse.builder().entries(putResultEntry).build()

    when(eventBridgeClient.putEvents(any[PutEventsRequest]())).thenReturn(CompletableFuture.completedFuture(putResult))

    val (probe, future) =
      TestSource.probe[PutEventsRequest].via(EventBridgePublisher.publishFlow()).toMat(Sink.seq)(Keep.both).run()

    probe
      .sendNext(requestDetail("eb-message-1"))
      .sendNext(requestDetail("eb-message-2"))
      .sendNext(requestDetail("eb-message-3"))
      .sendComplete()

    Await.result(future, 1.second) mustBe putResult :: putResult :: putResult :: Nil

    val expectedFirst = requestDetail("eb-message-1")
    verify(eventBridgeClient, times(1)).putEvents(meq(expectedFirst))

    val expectedSecond = requestDetail("eb-message-2")
    verify(eventBridgeClient, times(1)).putEvents(meq(expectedSecond))

    val expectedThird = requestDetail("eb-message-3")
    verify(eventBridgeClient, times(1)).putEvents(meq(expectedThird))
  }

  it should "publish multiple PublishRequest messages to multiple eventbridge topics" in {
    val putResult = resultResponse("event-id")

    when(eventBridgeClient.putEvents(any[PutEventsRequest]())).thenReturn(CompletableFuture.completedFuture(putResult))

    val (probe, future) =
      TestSource.probe[PutEventsRequest].via(EventBridgePublisher.publishFlow()).toMat(Sink.seq)(Keep.both).run()

    probe
      .sendNext(requestDetail("eb-message-1"))
      .sendNext(requestDetail("eb-message-2"))
      .sendNext(requestDetail("eb-message-3"))
      .sendComplete()

    Await.result(future, 1.second) mustBe putResult :: putResult :: putResult :: Nil

    val expectedFirst = requestDetail("eb-message-1")
    verify(eventBridgeClient, times(1)).putEvents(meq(expectedFirst))

    val expectedSecond = requestDetail("eb-message-2")
    verify(eventBridgeClient, times(1)).putEvents(meq(expectedSecond))

    val expectedThird = requestDetail("eb-message-3")
    verify(eventBridgeClient, times(1)).putEvents(meq(expectedThird))
  }

  it should "publish a single PutEventsRequestEntry message to eventbridge" in {
    val putRequestEntry = PutEventsRequestEntry.builder().detail("event-bridge-message").build()
    val publishRequest = requestDetail("event-bridge-message")
    val putResult = resultResponse("event-id")

    when(eventBridgeClient.putEvents(meq(publishRequest))).thenReturn(CompletableFuture.completedFuture(putResult))

    val (probe, future) =
      TestSource.probe[PutEventsRequestEntry].via(EventBridgePublisher.flow()).toMat(Sink.seq)(Keep.both).run()
    probe.sendNext(putRequestEntry).sendComplete()

    Await.result(future, 1.second) mustBe putResult :: Nil
    verify(eventBridgeClient, times(1)).putEvents(meq(publishRequest))
  }

  it should "publish multiple PutEventsRequestEntry messages to eventbridge" in {
    val putResult = resultResponse("event-id")

    when(eventBridgeClient.putEvents(any[PutEventsRequest]())).thenReturn(CompletableFuture.completedFuture(putResult))

    val (probe, future) =
      TestSource.probe[Seq[PutEventsRequestEntry]].via(EventBridgePublisher.flowSeq()).toMat(Sink.seq)(Keep.both).run()
    probe
      .sendNext(Seq(entryDetail("eb-message-1")))
      .sendNext(Seq(entryDetail("eb-message-2")))
      .sendNext(Seq(entryDetail("eb-message-3")))
      .sendComplete()

    Await.result(future, 1.second) mustBe putResult :: putResult :: putResult :: Nil

    val expectedFirst = requestDetail("eb-message-1")
    verify(eventBridgeClient, times(1)).putEvents(meq(expectedFirst))

    val expectedSecond = requestDetail("eb-message-2")
    verify(eventBridgeClient, times(1)).putEvents(meq(expectedSecond))

    val expectedThird = requestDetail("eb-message-3")
    verify(eventBridgeClient, times(1)).putEvents(meq(expectedThird))
  }

  it should "fail stage if EventBridgeAsyncClient request failed" in {
    val publishRequest = requestDetail("eb-message")

    val promise = new CompletableFuture[PutEventsResponse]()
    promise.completeExceptionally(new RuntimeException("publish error"))

    when(eventBridgeClient.putEvents(meq(publishRequest))).thenReturn(promise)

    val (probe, future) =
      TestSource.probe[Seq[PutEventsRequestEntry]].via(EventBridgePublisher.flowSeq()).toMat(Sink.seq)(Keep.both).run()
    probe.sendNext(Seq(entryDetail("eb-message"))).sendComplete()

    a[RuntimeException] should be thrownBy {
      Await.result(future, 1.second)
    }

    verify(eventBridgeClient, times(1)).putEvents(meq(publishRequest))
  }

  it should "fail stage if upstream failure occurs" in {
    case class MyCustomException(message: String) extends Exception(message)

    val (probe, future) =
      TestSource.probe[Seq[PutEventsRequestEntry]].via(EventBridgePublisher.flowSeq()).toMat(Sink.seq)(Keep.both).run()
    probe.sendError(MyCustomException("upstream failure"))

    a[MyCustomException] should be thrownBy {
      Await.result(future, 1.second)
    }

    verify(eventBridgeClient, never()).putEvents(any[PutEventsRequest]())
  }

}
