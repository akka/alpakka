/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.sqs.scaladsl

import java.util.concurrent.CompletableFuture
import java.util.function.Supplier

import akka.stream.alpakka.sqs.SqsSourceSettings
import akka.stream.testkit.scaladsl.TestSink
import org.mockito.ArgumentMatchers._
import org.mockito.Mockito._
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.scalatest.{FlatSpec, Matchers}
import org.scalatestplus.mockito.MockitoSugar.mock
import software.amazon.awssdk.services.sqs.SqsAsyncClient
import software.amazon.awssdk.services.sqs.model.{Message, ReceiveMessageRequest, ReceiveMessageResponse}

import scala.concurrent.duration._

class SqsSourceSpec extends FlatSpec with Matchers with DefaultTestContext {
  val defaultMessages = (1 to 10).map { i =>
    Message.builder().body(s"message $i").build()
  }

  "SqsSource" should "send a request and unwrap the response" in {
    implicit val sqsClient: SqsAsyncClient = mock[SqsAsyncClient]
    when(sqsClient.receiveMessage(any[ReceiveMessageRequest]))
      .thenReturn(
        CompletableFuture.completedFuture(
          ReceiveMessageResponse
            .builder()
            .messages(defaultMessages: _*)
            .build()
        )
      )

    val probe = SqsSource(
      "url",
      SqsSourceSettings.Defaults.withMaxBufferSize(10)
    ).runWith(TestSink.probe[Message])

    defaultMessages.foreach(probe.requestNext)

    /**
     * Invocations:
     * 1 - to initially fill he buffer
     * 2 - asynchronous call after the buffer is filled
     * 3 - buffer proxies pull when it's not full -> async stage provides the data and executes the next call
     */
    verify(sqsClient, times(3)).receiveMessage(any[ReceiveMessageRequest])
  }

  it should "buffer messages and acquire them fast with slow sqs" in {
    implicit val sqsClient: SqsAsyncClient = mock[SqsAsyncClient]
    val timeoutMs = 500
    val bufferToBatchRatio = 5

    when(sqsClient.receiveMessage(any[ReceiveMessageRequest]))
      .thenReturn(
        CompletableFuture.supplyAsync(
          new Supplier[ReceiveMessageResponse] {
            override def get(): ReceiveMessageResponse = {
              Thread.sleep(timeoutMs)

              ReceiveMessageResponse
                .builder()
                .messages(defaultMessages: _*)
                .build()
            }
          }
        )
      )

    val probe = SqsSource(
      "url",
      SqsSourceSettings.Defaults.withMaxBufferSize(SqsSourceSettings.Defaults.maxBatchSize * bufferToBatchRatio)
    ).runWith(TestSink.probe[Message])

    Thread.sleep(timeoutMs * (bufferToBatchRatio + 1))

    for {
      i <- 1 to (bufferToBatchRatio + 1)
      message <- defaultMessages
    } probe.requestNext(5.milliseconds) shouldEqual message
  }

  it should "enable throttling on emptyReceives and disable throttling when a new message arrives" in {
    implicit val sqsClient: SqsAsyncClient = mock[SqsAsyncClient]
    val firstWithDataCount = 30
    val thenEmptyCount = 20
    val parallelism = 10
    val timeout = 1.second
    var requestsCounter = 0
    when(sqsClient.receiveMessage(any[ReceiveMessageRequest]))
      .thenAnswer(new Answer[CompletableFuture[ReceiveMessageResponse]] {
        def answer(invocation: InvocationOnMock): CompletableFuture[ReceiveMessageResponse] = {
          val messages =
            if (requestsCounter >= firstWithDataCount && requestsCounter < firstWithDataCount + thenEmptyCount) {
              List.empty[Message]
            } else {
              defaultMessages
            }

          requestsCounter += 1
          CompletableFuture.completedFuture(
            ReceiveMessageResponse
              .builder()
              .messages(messages: _*)
              .build()
          )
        }
      })

    val probe = SqsSource(
      "url",
      SqsSourceSettings.Defaults
        .withMaxBufferSize(10)
        .withParallelRequests(10)
        .withWaitTime(timeout)
    ).runWith(TestSink.probe[Message])

    (1 to firstWithDataCount * 10).foreach(_ => probe.requestNext())

    verify(sqsClient, times(firstWithDataCount + parallelism + 3)).receiveMessage(any[ReceiveMessageRequest])

    // now the throttling kicks in
    probe.request(1)
    probe.expectNoMessage((thenEmptyCount - parallelism - 3) * timeout)
    verify(sqsClient, times(firstWithDataCount + thenEmptyCount)).receiveMessage(any[ReceiveMessageRequest])

    Thread.sleep(timeout.toMillis)

    // now the throttling is off
    probe.expectNext()

    (1 to 10).foreach(_ => probe.requestNext(timeout))
  }
}
