/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package docs.scaladsl

import java.util.UUID
import java.util.concurrent.{CompletableFuture, Future}

import akka.Done
import akka.stream.alpakka.sqs._
import akka.stream.alpakka.sqs.scaladsl.DefaultTestContext
import akka.stream.scaladsl.Keep
import akka.stream.testkit.scaladsl.TestSource
import com.amazonaws.handlers.AsyncHandler
import com.amazonaws.services.sqs.AmazonSQSAsync
import com.amazonaws.services.sqs.model._
import org.mockito.ArgumentMatchers._
import org.mockito.Mockito._
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.scalatest.mockito.MockitoSugar.mock
import org.scalatest.{FlatSpec, Matchers}

import scala.concurrent.Await
import scala.concurrent.duration._

class SettingsSnippetsSpec extends FlatSpec with Matchers with DefaultTestContext {

  "SqsBatchFlowSettings" should "construct settings" in {
    //#SqsBatchFlowSettings
    val batchSettings =
      SqsBatchFlowSettings()
        .withMaxBatchSize(10)
        .withMaxBatchWait(500.millis)
        .withConcurrentRequests(1)
    //#SqsBatchFlowSettings
    batchSettings.maxBatchSize should be(10)
  }

  "SqsSinkSettings" should "construct settings" in {
    //#SqsSinkSettings
    val sinkSettings =
      SqsSinkSettings()
        .withMaxInFlight(10)
    //#SqsSinkSettings
    sinkSettings.maxInFlight should be(10)
  }

  "SqsAckSinkSettings" should "construct settings" in {
    //#SqsAckSinkSettings
    val sinkSettings =
      SqsAckSinkSettings()
        .withMaxInFlight(10)
    //#SqsAckSinkSettings
    sinkSettings.maxInFlight should be(10)
  }

  "SqsBatchAckFlowSettings" should "construct settings" in {
    //#SqsBatchAckFlowSettings
    val batchSettings =
      SqsBatchAckFlowSettings()
        .withMaxBatchSize(10)
        .withMaxBatchWait(500.millis)
        .withConcurrentRequests(1)
    //#SqsBatchAckFlowSettings
    batchSettings.maxBatchSize should be(10)
  }

  "SqsSourceSettings" should "be constructed" in {
    //#SqsSourceSettings
    val settings = SqsSourceSettings()
      .withWaitTimeSeconds(20)
      .withMaxBufferSize(100)
      .withMaxBatchSize(10)
      .withAttributes(SenderId, SentTimestamp)
      .withMessageAttributes(MessageAttributeName.create("bar.*"))
      .withCloseOnEmptyReceive()
    //#SqsSourceSettings

    settings.maxBufferSize should be(100)

  }

}
