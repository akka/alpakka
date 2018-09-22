/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package docs.scaladsl
import java.nio.ByteOrder

import akka.stream.alpakka.mqtt.streaming._
import akka.util.{ByteString, ByteStringBuilder}
import org.scalatest.{Matchers, WordSpec}

class MqttCodecSpec extends WordSpec with Matchers {

  private implicit val byteOrder: ByteOrder = ByteOrder.BIG_ENDIAN
  import MqttCodec._

  "the codec" should {
    "encode/decode strings" in {
      val bsb: ByteStringBuilder = ByteString.newBuilder
      val bytes = "hi".encode(bsb).result()
      bytes.iterator.getShort shouldBe 2
      (bytes ++ ByteString("ignore")).iterator.decodeString() shouldBe Right("hi")
    }

    "underflow when decoding strings" in {
      ByteString.empty.iterator.decodeString() shouldBe Left(MqttCodec.BufferUnderflow)
    }

    "encode/decode reserved1 control packets" in {
      val bsb: ByteStringBuilder = ByteString.newBuilder
      val bytes = Reserved1.encode(bsb).result()
      bytes.size shouldBe 2
      (bytes ++ ByteString("ignore")).iterator.decodeControlPacket() shouldBe Right(Reserved1)
    }

    "encode/decode reserved2 control packets" in {
      val bsb: ByteStringBuilder = ByteString.newBuilder
      val bytes = Reserved2.encode(bsb).result()
      bytes.size shouldBe 2
      (bytes ++ ByteString("ignore")).iterator.decodeControlPacket() shouldBe Right(Reserved2)
    }

    "underflow when decoding control packets" in {
      ByteString.empty.iterator.decodeControlPacket() shouldBe Left(MqttCodec.BufferUnderflow)
    }

    "unknown packet type/flags when decoding control packets" in {
      ByteString.newBuilder.putByte(0x01).putByte(0x00).result().iterator.decodeControlPacket() shouldBe Left(
        UnknownPacketType(ControlPacketType(0), ControlPacketFlags(1))
      )
    }

    // TODO: Tests for connect
  }
}
