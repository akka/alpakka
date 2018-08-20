/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package docs.scaladsl

import akka.stream.alpakka.sqs._
import akka.stream.alpakka.sqs.scaladsl.DefaultTestContext
import org.scalatest.{FlatSpec, Matchers}

import scala.concurrent.duration._

class SettingsSnippetsSpec extends FlatSpec with Matchers with DefaultTestContext {

  "SqsPublishBatchSettings" should "construct settings" in {
    //#SqsBatchFlowSettings
    val batchSettings =
      SqsPublishBatchSettings()
        .withConcurrentRequests(1)
    //#SqsBatchFlowSettings
    batchSettings.concurrentRequests should be(1)
  }

  "SqsSinkSettings" should "construct settings" in {
    //#SqsSinkSettings
    val sinkSettings =
      SqsPublishSettings()
        .withMaxInFlight(10)
    //#SqsSinkSettings
    sinkSettings.maxInFlight should be(10)
  }

  "SqsAckSinkSettings" should "construct settings" in {
    //#SqsAckSinkSettings
    val sinkSettings =
      SqsAckSettings()
        .withMaxInFlight(10)
    //#SqsAckSinkSettings
    sinkSettings.maxInFlight should be(10)
  }

  "SqsBatchAckFlowSettings" should "construct settings" in {
    //#SqsBatchAckFlowSettings
    val batchSettings =
      SqsAckBatchSettings()
        .withMaxBatchSize(10)
        .withMaxBatchWait(500.millis)
        .withConcurrentRequests(1)
    //#SqsBatchAckFlowSettings
    batchSettings.maxBatchSize should be(10)
  }

}
