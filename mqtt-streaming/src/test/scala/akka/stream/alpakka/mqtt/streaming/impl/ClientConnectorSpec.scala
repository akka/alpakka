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

  "packet id allocator" should {
    "acquire a packet id" in {
      val replyTo = testKit.createTestProbe[PacketIdAllocator.Acquired]()
      val allocator = testKit.spawn(PacketIdAllocator())
      allocator ! PacketIdAllocator.Acquire(replyTo.ref)
      replyTo.expectMessage(PacketIdAllocator.Acquired(PacketId(1)))
    }

    "acquire two packet ids" in {
      val replyTo = testKit.createTestProbe[PacketIdAllocator.Acquired]()
      val allocator = testKit.spawn(PacketIdAllocator())
      allocator ! PacketIdAllocator.Acquire(replyTo.ref)
      allocator ! PacketIdAllocator.Acquire(replyTo.ref)
      replyTo.expectMessage(PacketIdAllocator.Acquired(PacketId(1)))
      replyTo.expectMessage(PacketIdAllocator.Acquired(PacketId(2)))
    }

    "acquire and release consecutive packet ids" in {
      val replyTo = testKit.createTestProbe[PacketIdAllocator.Acquired]()
      val allocator = testKit.spawn(PacketIdAllocator())

      allocator ! PacketIdAllocator.Acquire(replyTo.ref)
      allocator ! PacketIdAllocator.Acquire(replyTo.ref)
      replyTo.expectMessage(PacketIdAllocator.Acquired(PacketId(1)))
      replyTo.expectMessage(PacketIdAllocator.Acquired(PacketId(2)))

      allocator ! PacketIdAllocator.Release(PacketId(1))
      allocator ! PacketIdAllocator.Acquire(replyTo.ref)
      replyTo.expectMessage(PacketIdAllocator.Acquired(PacketId(3)))

      allocator ! PacketIdAllocator.Release(PacketId(2))
      allocator ! PacketIdAllocator.Release(PacketId(3))
      allocator ! PacketIdAllocator.Acquire(replyTo.ref)
      replyTo.expectMessage(PacketIdAllocator.Acquired(PacketId(1)))
    }
  }
}
