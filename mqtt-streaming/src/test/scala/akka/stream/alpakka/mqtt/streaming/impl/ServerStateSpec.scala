/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.mqtt.streaming.impl

import akka.actor.testkit.typed.scaladsl.ActorTestKit
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpec}

class ServerStateSpec extends WordSpec with Matchers with BeforeAndAfterAll {

  val testKit = ActorTestKit()
  override def afterAll(): Unit = testKit.shutdownTestKit()

  "publisher" should {
    "match topic filters" in {
      ClientConnection.matchTopicFilter("sport/tennis/player1", "sport/tennis/player1") shouldBe true

      ClientConnection.matchTopicFilter("sport/tennis/player1/#", "sport/tennis/player1") shouldBe true
      ClientConnection.matchTopicFilter("sport/tennis/player1/#", "sport/tennis/player1/ranking") shouldBe true
      ClientConnection.matchTopicFilter("sport/tennis/player1/#", "sport/tennis/player1/score/wimbledon") shouldBe true

      ClientConnection.matchTopicFilter("sport/#", "sport") shouldBe true
      ClientConnection.matchTopicFilter("#", "sport") shouldBe true
      ClientConnection.matchTopicFilter("sport/tennis/#", "sport/tennis") shouldBe true
      ClientConnection.matchTopicFilter("sport/tennis#", "sport/tennis") shouldBe false
      ClientConnection.matchTopicFilter("sport/tennis/#/ranking", "sport/tennis/player1/ranking") shouldBe false

      ClientConnection.matchTopicFilter("sport/tennis/+", "sport/tennis/player1") shouldBe true
      ClientConnection.matchTopicFilter("sport/tennis/+", "sport/tennis/player1/tranking") shouldBe false

      ClientConnection.matchTopicFilter("sport/+", "sport") shouldBe false
      ClientConnection.matchTopicFilter("sport/+", "sport/") shouldBe true

      ClientConnection.matchTopicFilter("+", "sport") shouldBe true
      ClientConnection.matchTopicFilter("+/tennis/#", "sport/tennis") shouldBe true
      ClientConnection.matchTopicFilter("sport+", "sport") shouldBe false
    }

    "match topic filters that are topic filters" in {
      ClientConnection.matchTopicFilter("#", "#") shouldBe true
      ClientConnection.matchTopicFilter("#", "/a/#") shouldBe true
      ClientConnection.matchTopicFilter("+", "+") shouldBe true
      ClientConnection.matchTopicFilter("/+/#", "/+/#") shouldBe true
    }
  }
}
