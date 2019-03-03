/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.mqtt.streaming
package impl

import akka.actor.testkit.typed.scaladsl.ActorTestKit
import akka.util.ByteString
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpec}

import scala.concurrent.Promise
import scala.concurrent.duration._

class RequestStateSpec extends WordSpec with Matchers with BeforeAndAfterAll with ScalaFutures {

  val testKit = ActorTestKit()
  override def afterAll(): Unit = testKit.shutdownTestKit()

  "publisher" should {
    "match topic filters" in {
      Topics.filter("sport/tennis/player1", "sport/tennis/player1") shouldBe true

      Topics.filter("sport/tennis/player1/#", "sport/tennis/player1") shouldBe true
      Topics.filter("sport/tennis/player1/#", "sport/tennis/player1/ranking") shouldBe true
      Topics.filter("sport/tennis/player1/#", "sport/tennis/player1/score/wimbledon") shouldBe true

      Topics.filter("sport/#", "sport") shouldBe true
      Topics.filter("#", "sport") shouldBe true
      Topics.filter("sport/tennis/#", "sport/tennis") shouldBe true
      Topics.filter("sport/tennis#", "sport/tennis") shouldBe false
      Topics.filter("sport/tennis/#/ranking", "sport/tennis/player1/ranking") shouldBe false

      Topics.filter("sport/tennis/+", "sport/tennis/player1") shouldBe true
      Topics.filter("sport/tennis/+", "sport/tennis/player1/tranking") shouldBe false

      Topics.filter("sport/+", "sport") shouldBe false
      Topics.filter("sport/+", "sport/") shouldBe true

      Topics.filter("+", "sport") shouldBe true
      Topics.filter("+/tennis/#", "sport/tennis") shouldBe true
      Topics.filter("sport+", "sport") shouldBe false
    }

    "match topic filters that are topic filters" in {
      Topics.filter("#", "#") shouldBe true
      Topics.filter("#", "/a/#") shouldBe true
      Topics.filter("+", "+") shouldBe true
      Topics.filter("/+/#", "/+/#") shouldBe true
    }
  }

  "local packet router" should {
    "calculate the next packet id correctly" in {
      LocalPacketRouter.findNextPacketId(
        Map.empty,
        PacketId(1)
      ) shouldBe Some(PacketId(2))
    }

    "calculate the next packet id correctly, accounting for wrap around" in {
      LocalPacketRouter.findNextPacketId(
        Map.empty,
        LocalPacketRouter.MaxPacketId
      ) shouldBe Some(LocalPacketRouter.MinPacketId)
    }

    "calculate the next packet id correctly, accounting for used ids" in {
      LocalPacketRouter.findNextPacketId(
        Map(
          PacketId(2) -> testKit.spawn(LocalPacketRouter[String])
        ),
        PacketId(1)
      ) shouldBe Some(PacketId(3))
    }

    "acquire a packet id" in {
      val registrant = testKit.createTestProbe[String]()
      val reply = Promise[LocalPacketRouter.Registered]()
      val router = testKit.spawn(LocalPacketRouter[String])
      router ! LocalPacketRouter.Register(registrant.ref, reply)
      reply.future.futureValue shouldBe LocalPacketRouter.Registered(PacketId(1))
    }

    "acquire two packet ids" in {
      val registrant = testKit.createTestProbe[String]()
      val reply1 = Promise[LocalPacketRouter.Registered]
      val reply2 = Promise[LocalPacketRouter.Registered]
      val router = testKit.spawn(LocalPacketRouter[String])
      router ! LocalPacketRouter.Register(registrant.ref, reply1)
      router ! LocalPacketRouter.Register(registrant.ref, reply2)
      reply1.future.futureValue shouldBe LocalPacketRouter.Registered(PacketId(1))
      reply2.future.futureValue shouldBe LocalPacketRouter.Registered(PacketId(2))
    }

    "acquire consecutive packet ids" in {
      val registrant = testKit.createTestProbe[String]()
      val reply1 = Promise[LocalPacketRouter.Registered]
      val reply2 = Promise[LocalPacketRouter.Registered]
      val reply3 = Promise[LocalPacketRouter.Registered]
      val reply4 = Promise[LocalPacketRouter.Registered]
      val router = testKit.spawn(LocalPacketRouter[String])

      router ! LocalPacketRouter.Register(registrant.ref, reply1)
      router ! LocalPacketRouter.Register(registrant.ref, reply2)
      reply1.future.futureValue shouldBe LocalPacketRouter.Registered(PacketId(1))
      reply2.future.futureValue shouldBe LocalPacketRouter.Registered(PacketId(2))

      router ! LocalPacketRouter.Unregister(PacketId(1))
      router ! LocalPacketRouter.Register(registrant.ref, reply3)
      reply3.future.futureValue shouldBe LocalPacketRouter.Registered(PacketId(3))

      router ! LocalPacketRouter.Unregister(PacketId(2))
      router ! LocalPacketRouter.Unregister(PacketId(3))
      router ! LocalPacketRouter.Register(registrant.ref, reply4)
      reply4.future.futureValue shouldBe LocalPacketRouter.Registered(PacketId(4))
    }

    "route a packet" in {
      val registrant = testKit.createTestProbe[String]()
      val reply = Promise[LocalPacketRouter.Registered]()
      val router = testKit.spawn(LocalPacketRouter[String])
      router ! LocalPacketRouter.Register(registrant.ref, reply)
      val registered = reply.future.futureValue
      val failureReply = Promise[String]
      router ! LocalPacketRouter.Route(registered.packetId, "some-packet", failureReply)
      registrant.expectMessage("some-packet")
      failureReply.future.isCompleted shouldBe false
    }

    "fail to route a packet" in {
      val reply = Promise[LocalPacketRouter.Registered]()
      val router = testKit.spawn(LocalPacketRouter[String])
      router ! LocalPacketRouter.Route(PacketId(1), "some-packet", reply)
      reply.future.failed.futureValue shouldBe LocalPacketRouter.CannotRoute(PacketId(1))
    }
  }

  "remote packet router" should {

    "route a packet" in {
      val clientId = "some-client"
      val packetId = PacketId(1)

      val connectionId = ByteString("some-connection")

      val registrant = testKit.createTestProbe[String]()
      val registerReply = Promise[RemotePacketRouter.Registered.type]()
      val failureReply1 = Promise[String]
      val failureReply2 = Promise[String]
      val failureReply3 = Promise[String]
      val failureReply4 = Promise[String]
      val router = testKit.spawn(RemotePacketRouter[String])

      router ! RemotePacketRouter.Register(registrant.ref, Some(clientId), packetId, registerReply)
      registerReply.future.futureValue shouldBe RemotePacketRouter.Registered

      router ! RemotePacketRouter.Route(Some(clientId), packetId, "some-packet", failureReply1)
      registrant.expectMessage("some-packet")
      failureReply1.future.isCompleted shouldBe false

      router ! RemotePacketRouter.RegisterConnection(connectionId, clientId)
      router ! RemotePacketRouter.RouteViaConnection(connectionId, packetId, "some-packet2", failureReply3)
      registrant.expectMessage("some-packet2")
      failureReply3.future.isCompleted shouldBe false

      router ! RemotePacketRouter.UnregisterConnection(connectionId)
      router ! RemotePacketRouter.RouteViaConnection(connectionId, packetId, "some-packet2", failureReply4)
      failureReply4.future.failed.futureValue shouldBe RemotePacketRouter.CannotRoute(packetId)
      registrant.expectNoMessage(100.millis)

      router ! RemotePacketRouter.Unregister(Some(clientId), packetId)
      router ! RemotePacketRouter.Route(Some(clientId), packetId, "some-packet", failureReply2)
      failureReply2.future.failed.futureValue shouldBe RemotePacketRouter.CannotRoute(packetId)
      registrant.expectNoMessage(100.millis)

    }
  }
}
