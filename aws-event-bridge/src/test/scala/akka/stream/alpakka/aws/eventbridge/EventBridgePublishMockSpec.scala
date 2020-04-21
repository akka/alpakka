/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.aws.eventbridge

import java.util.concurrent.CompletableFuture

import akka.stream.alpakka.aws.eventbridge.scaladsl.EventBridgePublisher
import akka.stream.scaladsl.{Keep, Sink}
import akka.stream.testkit.scaladsl.TestSource
import org.mockito.ArgumentMatchers.{any, eq => meq}
import org.mockito.Mockito._
import org.scalatest.flatspec._
import org.scalatest.matchers.must.Matchers
import software.amazon.awssdk.services.eventbridge.model._

import scala.concurrent.Await
import scala.concurrent.duration._

class EventBridgePublishMockSpec extends AnyFlatSpec with DefaultTestContext with Matchers {

  private def entryDetail(detail: String, eventBusName: Option[String] = None): PutEventsRequestEntry = {
    val entry = PutEventsRequestEntry.builder().detail(detail)
    eventBusName.map(entry.eventBusName(_))
    entry.build()
  }

  private def requestDetail(eventBusName: Option[String], details: String*): PutEventsRequest =
    PutEventsRequest
      .builder()
      .entries(
        details.map(detail => entryDetail(detail, eventBusName)): _*
      )
      .build()

  private def resultResponse(eventId: String): PutEventsResponse = {
    val putResultEntry = PutEventsResultEntry.builder().eventId(eventId).build()
    PutEventsResponse.builder().entries(putResultEntry).build()
  }

  it should "publish a single PutEventsEntry to eventBridge" in {
    val putRequestEntry = entryDetail("event-bridge-message")
    val putRequest = requestDetail(None, "event-bridge-message")
    val putResult = resultResponse("event-id")

    when(eventBridgeClient.putEvents(putRequest)).thenReturn(CompletableFuture.completedFuture(putResult))

    val (probe, future) =
      TestSource.probe[PutEventsRequestEntry].via(EventBridgePublisher.flow()).toMat(Sink.seq)(Keep.both).run()

    probe.sendNext(putRequestEntry).sendComplete()

    Await.result(future, 1.second) mustBe putResult :: Nil

    verify(eventBridgeClient, times(1)).putEvents(meq(putRequest))
  }

  it should "publish multiple PutEventsRequestEntry objects with dynamic eventbus name" in {
    val putResult = resultResponse("event-id")

    when(eventBridgeClient.putEvents(any[PutEventsRequest]())).thenReturn(CompletableFuture.completedFuture(putResult))

    val (probe, future) =
      TestSource.probe[PutEventsRequestEntry].via(EventBridgePublisher.flow()).toMat(Sink.seq)(Keep.both).run()

    probe
      .sendNext(entryDetail("eb-message-1"))
      .sendNext(entryDetail("eb-message-2", Some("topic2")))
      .sendNext(entryDetail("eb-message-3", Some("topic3")))
      .sendComplete()

    Await.result(future, 1.second) mustBe putResult :: putResult :: putResult :: Nil

    val expectedFirst = requestDetail(None, "eb-message-1")
    verify(eventBridgeClient, times(1)).putEvents(meq(expectedFirst))

    val expectedSecond = requestDetail(Some("topic2"), "eb-message-2")
    verify(eventBridgeClient, times(1)).putEvents(meq(expectedSecond))

    val expectedThird = requestDetail(Some("topic3"), "eb-message-3")
    verify(eventBridgeClient, times(1)).putEvents(meq(expectedThird))
  }

  it should "publish multiple PutEventsRequestEntry objects to eventbridge in batch" in {
    val putResult = resultResponse("event-id")

    when(eventBridgeClient.putEvents(any[PutEventsRequest]())).thenReturn(CompletableFuture.completedFuture(putResult))

    val (probe, future) =
      TestSource.probe[Seq[PutEventsRequestEntry]].via(EventBridgePublisher.flowSeq()).toMat(Sink.seq)(Keep.both).run()
    probe
      .sendNext(Seq(entryDetail("eb-message-1"), entryDetail("eb-message-2"), entryDetail("eb-message-3")))
      .sendComplete()

    Await.result(future, 1.second) mustBe putResult :: Nil

    val expected = requestDetail(None, "eb-message-1", "eb-message-2", "eb-message-3")
    verify(eventBridgeClient, times(1)).putEvents(meq(expected))

  }

  it should "fail stage if EventBridgeAsyncClient request failed" in {
    val publishRequest = requestDetail(None, "eb-message")

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
