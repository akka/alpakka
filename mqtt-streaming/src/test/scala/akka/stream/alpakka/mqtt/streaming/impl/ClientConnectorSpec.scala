/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.mqtt.streaming
package impl
import akka.actor.testkit.typed.scaladsl.ActorTestKit
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpec}

class ClientConnectorSpec extends WordSpec with Matchers with BeforeAndAfterAll {

  val testKit = ActorTestKit()
  override def afterAll(): Unit = testKit.shutdownTestKit()

  "packet id router" should {
    "acquire a packet id" in {
      val registrant = testKit.createTestProbe[String]()
      val replyTo = testKit.createTestProbe[PacketRouter.Registered]()
      val router = testKit.spawn(PacketRouter[String])
      router ! PacketRouter.Register(registrant.ref, replyTo.ref)
      replyTo.expectMessage(PacketRouter.Registered(PacketId(1)))
    }

    "acquire two packet ids" in {
      val registrant = testKit.createTestProbe[String]()
      val replyTo = testKit.createTestProbe[PacketRouter.Registered]()
      val router = testKit.spawn(PacketRouter[String])
      router ! PacketRouter.Register(registrant.ref, replyTo.ref)
      router ! PacketRouter.Register(registrant.ref, replyTo.ref)
      replyTo.expectMessage(PacketRouter.Registered(PacketId(1)))
      replyTo.expectMessage(PacketRouter.Registered(PacketId(2)))
    }

    "acquire and release consecutive packet ids" in {
      val registrant = testKit.createTestProbe[String]()
      val replyTo = testKit.createTestProbe[PacketRouter.Registered]()
      val router = testKit.spawn(PacketRouter[String])

      router ! PacketRouter.Register(registrant.ref, replyTo.ref)
      router ! PacketRouter.Register(registrant.ref, replyTo.ref)
      replyTo.expectMessage(PacketRouter.Registered(PacketId(1)))
      replyTo.expectMessage(PacketRouter.Registered(PacketId(2)))

      router ! PacketRouter.Unregister(PacketId(1))
      router ! PacketRouter.Register(registrant.ref, replyTo.ref)
      replyTo.expectMessage(PacketRouter.Registered(PacketId(3)))

      router ! PacketRouter.Unregister(PacketId(2))
      router ! PacketRouter.Unregister(PacketId(3))
      router ! PacketRouter.Register(registrant.ref, replyTo.ref)
      replyTo.expectMessage(PacketRouter.Registered(PacketId(1)))
    }

    "router a packet" in {
      val registrant = testKit.createTestProbe[String]()
      val replyTo = testKit.createTestProbe[PacketRouter.Registered]()
      val router = testKit.spawn(PacketRouter[String])
      router ! PacketRouter.Register(registrant.ref, replyTo.ref)
      val registered = replyTo.expectMessageType[PacketRouter.Registered]
      router ! PacketRouter.Route(registered.packetId, "some-packet")
      registrant.expectMessage("some-packet")
    }
  }

  "subscriber" should {
    "match topic filters" in {
      Subscriber.matchTopicFilter("sport/tennis/player1", "sport/tennis/player1") shouldBe true

      Subscriber.matchTopicFilter("sport/tennis/player1/#", "sport/tennis/player1") shouldBe true
      Subscriber.matchTopicFilter("sport/tennis/player1/#", "sport/tennis/player1/ranking") shouldBe true
      Subscriber.matchTopicFilter("sport/tennis/player1/#", "sport/tennis/player1/score/wimbledon") shouldBe true

      Subscriber.matchTopicFilter("sport/#", "sport") shouldBe true
      Subscriber.matchTopicFilter("#", "sport") shouldBe true
      Subscriber.matchTopicFilter("sport/tennis/#", "sport/tennis") shouldBe true
      Subscriber.matchTopicFilter("sport/tennis#", "sport/tennis") shouldBe false
      Subscriber.matchTopicFilter("sport/tennis/#/ranking", "sport/tennis/player1/ranking") shouldBe false

      Subscriber.matchTopicFilter("sport/tennis/+", "sport/tennis/player1") shouldBe true
      Subscriber.matchTopicFilter("sport/tennis/+", "sport/tennis/player1/tranking") shouldBe false

      Subscriber.matchTopicFilter("sport/+", "sport") shouldBe false
      Subscriber.matchTopicFilter("sport/+", "sport/") shouldBe true

      Subscriber.matchTopicFilter("+", "sport") shouldBe true
      Subscriber.matchTopicFilter("+/tennis/#", "sport/tennis") shouldBe true
      Subscriber.matchTopicFilter("sport+", "sport") shouldBe false
    }
  }
}
