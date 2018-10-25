/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.mqtt.streaming
package impl

import akka.actor.testkit.typed.scaladsl.ActorTestKit
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpec}

import scala.concurrent.duration._

class RequestStateSpec extends WordSpec with Matchers with BeforeAndAfterAll {

  val testKit = ActorTestKit()
  override def afterAll(): Unit = testKit.shutdownTestKit()

  "local packet router" should {
    "acquire a packet id" in {
      val registrant = testKit.createTestProbe[String]()
      val replyTo = testKit.createTestProbe[LocalPacketRouter.Registered]()
      val router = testKit.spawn(LocalPacketRouter[String])
      router ! LocalPacketRouter.Register(registrant.ref, replyTo.ref)
      replyTo.expectMessage(LocalPacketRouter.Registered(PacketId(1)))
    }

    "acquire two packet ids" in {
      val registrant = testKit.createTestProbe[String]()
      val replyTo = testKit.createTestProbe[LocalPacketRouter.Registered]()
      val router = testKit.spawn(LocalPacketRouter[String])
      router ! LocalPacketRouter.Register(registrant.ref, replyTo.ref)
      router ! LocalPacketRouter.Register(registrant.ref, replyTo.ref)
      replyTo.expectMessage(LocalPacketRouter.Registered(PacketId(1)))
      replyTo.expectMessage(LocalPacketRouter.Registered(PacketId(2)))
    }

    "acquire and release consecutive packet ids" in {
      val registrant = testKit.createTestProbe[String]()
      val replyTo = testKit.createTestProbe[LocalPacketRouter.Registered]()
      val router = testKit.spawn(LocalPacketRouter[String])

      router ! LocalPacketRouter.Register(registrant.ref, replyTo.ref)
      router ! LocalPacketRouter.Register(registrant.ref, replyTo.ref)
      replyTo.expectMessage(LocalPacketRouter.Registered(PacketId(1)))
      replyTo.expectMessage(LocalPacketRouter.Registered(PacketId(2)))

      router ! LocalPacketRouter.Unregister(PacketId(1))
      router ! LocalPacketRouter.Register(registrant.ref, replyTo.ref)
      replyTo.expectMessage(LocalPacketRouter.Registered(PacketId(3)))

      router ! LocalPacketRouter.Unregister(PacketId(2))
      router ! LocalPacketRouter.Unregister(PacketId(3))
      router ! LocalPacketRouter.Register(registrant.ref, replyTo.ref)
      replyTo.expectMessage(LocalPacketRouter.Registered(PacketId(1)))
    }

    "route a packet" in {
      val registrant = testKit.createTestProbe[String]()
      val replyTo = testKit.createTestProbe[LocalPacketRouter.Registered]()
      val router = testKit.spawn(LocalPacketRouter[String])
      router ! LocalPacketRouter.Register(registrant.ref, replyTo.ref)
      val registered = replyTo.expectMessageType[LocalPacketRouter.Registered]
      router ! LocalPacketRouter.Route(registered.packetId, "some-packet")
      registrant.expectMessage("some-packet")
    }
  }

  "remote packet router" should {

    "route a packet" in {
      val packetId = PacketId(1)

      val registrant = testKit.createTestProbe[String]()
      val replyTo = testKit.createTestProbe[RemotePacketRouter.Registered.type]()
      val router = testKit.spawn(RemotePacketRouter[String])

      router ! RemotePacketRouter.Register(registrant.ref, packetId, replyTo.ref)
      replyTo.expectMessage(RemotePacketRouter.Registered)

      router ! RemotePacketRouter.Route(packetId, "some-packet")
      registrant.expectMessage("some-packet")

      router ! RemotePacketRouter.Unregister(packetId)
      router ! RemotePacketRouter.Route(packetId, "some-packet")
      registrant.expectNoMessage(1.second)

    }
  }
}
