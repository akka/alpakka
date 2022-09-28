/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.mqtt.streaming
package impl

import akka.actor.ActorSystem
import akka.stream.alpakka.testkit.scaladsl.LogCapturing
import akka.stream.scaladsl.{Keep, Source}
import akka.stream.testkit.scaladsl.TestSink
import akka.stream.testkit.scaladsl.TestSource
import akka.testkit.TestKit
import akka.util.ByteString
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

class MqttFrameStageSpec
    extends TestKit(ActorSystem("MqttFrameStageSpec"))
    with AnyWordSpecLike
    with Matchers
    with BeforeAndAfterAll
    with LogCapturing {

  val MaxPacketSize = 100

  "framing" should {
    "frame a packet with just a fixed header" in {
      val bytes = ByteString.newBuilder.putByte(0).putByte(0).result()
      Source
        .single(bytes)
        .via(new MqttFrameStage(MaxPacketSize))
        .runWith(TestSink()(system))
        .request(1)
        .expectNext(bytes)
        .expectComplete()
    }

    "frame a packet with a fixed and variable header" in {
      val bytes = ByteString.newBuilder.putByte(0).putByte(1).putByte(0).result()
      Source
        .single(bytes)
        .via(new MqttFrameStage(MaxPacketSize))
        .runWith(TestSink()(system))
        .request(1)
        .expectNext(bytes)
        .expectComplete()
    }

    "frame two packets from bytes" in {
      val bytes = ByteString.newBuilder.putByte(0).putByte(1).putByte(0).result()
      Source
        .single(bytes ++ bytes)
        .via(new MqttFrameStage(MaxPacketSize))
        .runWith(TestSink()(system))
        .request(2)
        .expectNext(bytes, bytes)
        .expectComplete()
    }

    "frame a packet where its length bytes are split" in {
      val bytes0 = ByteString.newBuilder.putByte(0).putByte(0x80.toByte).result()
      val bytes1 = ByteString.newBuilder.putByte(1).putBytes(Array.ofDim(0x80)).result()

      val (pub, sub) =
        TestSource()(system)
          .via(new MqttFrameStage(MaxPacketSize * 2))
          .toMat(TestSink()(system))(Keep.both)
          .run()

      pub.sendNext(bytes0)

      sub.request(1)

      pub.sendNext(bytes1).sendComplete()

      sub
        .expectNext(bytes0 ++ bytes1)
        .expectComplete()
    }

    "fail if packet size exceeds max" in {
      val bytes = ByteString.newBuilder.putByte(0).putByte(MaxPacketSize.toByte).putByte(0).result()
      val ex =
        Source
          .single(bytes)
          .via(new MqttFrameStage(MaxPacketSize))
          .runWith(TestSink()(system))
          .request(1)
          .expectError()
      ex.getMessage shouldBe s"Max packet size of $MaxPacketSize exceeded with ${MaxPacketSize + 2}"
    }
  }

  override def afterAll(): Unit =
    TestKit.shutdownActorSystem(system)
}
