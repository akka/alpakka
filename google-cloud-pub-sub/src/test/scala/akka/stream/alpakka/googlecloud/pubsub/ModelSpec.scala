/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.pubsub

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import scala.collection.immutable.Seq
import java.time.Instant

import akka.stream.alpakka.testkit.scaladsl.LogCapturing

class ModelSpec extends AnyFunSuite with Matchers with LogCapturing {

  val publishMessage1 = PublishMessage("abcde", Map("k1" -> "v1", "k2" -> "v2"))
  val publishMessage2 = PublishMessage("abcde", Map("k1" -> "v1"))
  val publishMessage3 = PublishMessage("abcde")
  val publishMessage4 = PublishMessage("abcd", Map("k1" -> "v1", "k2" -> "v2"))
  val publishMessage5 = PublishMessage("abcde", Map("k1" -> "v1", "k2" -> "v2"))

  test("PublishMessage equals, hashCode") {

    publishMessage1 shouldNot be(publishMessage2)
    publishMessage1 shouldNot be(publishMessage3)
    publishMessage1 shouldNot be(publishMessage4)
    publishMessage1 shouldBe publishMessage5

    publishMessage1.hashCode shouldNot be(publishMessage2.hashCode)
    publishMessage1.hashCode shouldNot be(publishMessage3.hashCode)
    publishMessage1.hashCode shouldNot be(publishMessage4.hashCode)
    publishMessage1.hashCode shouldBe publishMessage5.hashCode
  }

  test("PublishMessage toString") {
    publishMessage1.toString shouldBe
    "PublishMessage(data=abcde,attributes=Some(Map(k1 -> v1, k2 -> v2)))"
  }

  val pubSubMessage1 = PubSubMessage(Some("data"), Some(Map("k1" -> "v1")), "Id-1", Instant.ofEpochMilli(0L))
  val pubSubMessage2 = PubSubMessage(Some("data2"), Some(Map("k1" -> "v1")), "Id-1", Instant.ofEpochMilli(0L))
  val pubSubMessage3 = PubSubMessage(Some("data"), Some(Map("k1" -> "v2")), "Id-1", Instant.ofEpochMilli(0L))
  val pubSubMessage4 = PubSubMessage(Some("data"), Some(Map("k1" -> "v1")), "Id-2", Instant.ofEpochMilli(0L))
  val pubSubMessage5 = PubSubMessage(Some("data"), Some(Map("k1" -> "v1")), "Id-1", Instant.ofEpochMilli(1L))
  val pubSubMessage6 = PubSubMessage(Some("data"), Some(Map("k1" -> "v1")), "Id-1", Instant.ofEpochMilli(0L))

  test("PubSubMessage equals, hashCode") {
    pubSubMessage1 shouldNot be(pubSubMessage2)
    pubSubMessage1 shouldNot be(pubSubMessage3)
    pubSubMessage1 shouldNot be(pubSubMessage4)
    pubSubMessage1 shouldNot be(pubSubMessage5)
    pubSubMessage1 shouldBe pubSubMessage6

    pubSubMessage1.hashCode shouldNot be(pubSubMessage2.hashCode)
    pubSubMessage1.hashCode shouldNot be(pubSubMessage3.hashCode)
    pubSubMessage1.hashCode shouldNot be(pubSubMessage4.hashCode)
    pubSubMessage1.hashCode shouldNot be(pubSubMessage5.hashCode)
    pubSubMessage1.hashCode shouldBe pubSubMessage6.hashCode
  }

  test("PubSubMessage toString") {
    pubSubMessage1.toString shouldBe "PubSubMessage(data=Some(data),attributes=Some(Map(k1 -> v1)),messageId=Id-1,publishTime=1970-01-01T00:00:00Z)"
  }

  val receivedMessage1 = ReceivedMessage("1", pubSubMessage1)
  val receivedMessage2 = ReceivedMessage("2", pubSubMessage1)
  val receivedMessage3 = ReceivedMessage("1", pubSubMessage2)
  val receivedMessage4 = ReceivedMessage("1", pubSubMessage1)

  test("ReceivedMessage equals, hashCode") {
    receivedMessage1 shouldNot be(receivedMessage2)
    receivedMessage1 shouldNot be(receivedMessage3)
    receivedMessage1 shouldBe receivedMessage4

    receivedMessage1.hashCode shouldNot be(receivedMessage2.hashCode)
    receivedMessage1.hashCode shouldNot be(receivedMessage3.hashCode)
    receivedMessage1.hashCode shouldBe receivedMessage4.hashCode
  }

  test("ReceivedMessage toString") {
    receivedMessage1.toString shouldBe
    "ReceivedMessage(ackId=1,message=PubSubMessage(data=Some(data),attributes=Some(Map(k1 -> v1)),messageId=Id-1,publishTime=1970-01-01T00:00:00Z))"
  }

  val acknowledgeRequest1 = AcknowledgeRequest()
  val acknowledgeRequest2 = AcknowledgeRequest("id1")
  val acknowledgeRequest3 = AcknowledgeRequest("id1", "id2")
  val acknowledgeRequest4 = AcknowledgeRequest("id2", "id1")
  val acknowledgeRequest5 = AcknowledgeRequest("id1", "id2")

  test("AcknowledgeRequest equals, hashCode and toString") {
    acknowledgeRequest1 shouldNot be(acknowledgeRequest2)
    acknowledgeRequest1 shouldNot be(acknowledgeRequest3)
    acknowledgeRequest1 shouldNot be(acknowledgeRequest4)
    acknowledgeRequest3 should be(acknowledgeRequest5)

    acknowledgeRequest1.hashCode shouldNot be(acknowledgeRequest2.hashCode)
    acknowledgeRequest1.hashCode shouldNot be(acknowledgeRequest3.hashCode)
    acknowledgeRequest1.hashCode shouldNot be(acknowledgeRequest4.hashCode)
    acknowledgeRequest3.hashCode shouldBe acknowledgeRequest5.hashCode
  }

  test("AcknowledgeRequest toString") {
    acknowledgeRequest3.toString shouldBe "AcknowledgeRequest([id1,id2])"
  }

  val publishRequest1 = PublishRequest(Seq(publishMessage1))
  val publishRequest2 = PublishRequest(Seq(publishMessage2))
  val publishRequest3 = PublishRequest(Seq(publishMessage1))

  test("PublishRequest equals, hashCode") {
    publishRequest1 shouldNot be(publishRequest2)
    publishRequest1 shouldBe publishRequest3

    publishRequest1.hashCode shouldNot be(publishRequest2.hashCode)
    publishRequest1.hashCode shouldBe publishRequest3.hashCode
  }

  test("PublishRequest toString") {
    publishRequest1.toString shouldBe "PublishRequest([PublishMessage(data=abcde,attributes=Some(Map(k1 -> v1, k2 -> v2)))])"
  }

  private val publishResponse1 = PublishResponse(Seq.empty[String])
  private val publishResponse2 = PublishResponse(Seq("id"))
  private val publishResponse3 = PublishResponse(Seq("id1", "id2"))
  private val publishResponse4 = PublishResponse(Seq("id2", "id1"))
  private val publishResponse5 = PublishResponse(Seq("id1", "id2"))

  test("PublishResponse equals, hashCode") {
    publishResponse1 shouldNot be(publishResponse2)
    publishResponse2 shouldNot be(publishResponse3)
    publishResponse3 shouldNot be(publishResponse4)
    publishResponse3 shouldBe publishResponse5

    publishResponse1.hashCode shouldNot be(publishResponse2.hashCode)
    publishResponse2.hashCode shouldNot be(publishResponse3.hashCode)
    publishResponse3.hashCode shouldNot be(publishResponse4.hashCode)
    publishResponse3.hashCode shouldBe publishResponse5.hashCode
  }

  test("PublishResponse toString") {
    publishResponse3.toString shouldBe "PublishResponse([id1,id2])"
  }

  private val pullResponse1 = PullResponse(Some(Seq(receivedMessage1)))
  private val pullResponse2 = PullResponse(None)
  private val pullResponse3 = PullResponse(Some(Seq(receivedMessage1)))

  test("PullResponse equals, hashCode") {
    pullResponse1 shouldNot be(pullResponse2)
    pullResponse1 shouldBe pullResponse3

    pullResponse1.hashCode shouldNot be(pullResponse2.hashCode)
    pullResponse1.hashCode shouldBe pullResponse3.hashCode
  }

  test("PullResponse toString") {
    pullResponse1.toString shouldBe "PullResponse(Some([ReceivedMessage(ackId=1,message=PubSubMessage(data=Some(data),attributes=Some(Map(k1 -> v1)),messageId=Id-1,publishTime=1970-01-01T00:00:00Z))]))"
  }
}
