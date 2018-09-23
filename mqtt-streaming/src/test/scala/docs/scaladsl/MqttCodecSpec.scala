/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package docs.scaladsl
import java.nio.ByteOrder

import akka.stream.alpakka.mqtt.streaming._
import akka.util.{ByteString, ByteStringBuilder}
import org.scalatest.{Matchers, WordSpec}

import scala.concurrent.duration._

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
      val bytes = Reserved1.encode(bsb, 0).result()
      bytes.size shouldBe 2
      (bytes ++ ByteString("ignore")).iterator.decodeControlPacket() shouldBe Right(Reserved1)
    }

    "encode/decode reserved2 control packets" in {
      val bsb: ByteStringBuilder = ByteString.newBuilder
      val bytes = Reserved2.encode(bsb, 0).result()
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

    "encode/decode connect control packets" in {
      val bsb: ByteStringBuilder = ByteString.newBuilder
      val packet = Connect(
        Connect.Mqtt,
        Connect.v311,
        "some-client-id",
        ConnectFlags.CleanSession | ConnectFlags.WillFlag | ConnectFlags.WillQoS | ConnectFlags.WillRetain | ConnectFlags.PasswordFlag | ConnectFlags.UsernameFlag,
        1.second,
        Some("some-will-topic"),
        Some("some-will-message"),
        Some("some-username"),
        Some("some-password")
      )
      val bytes = packet.encode(bsb).result()
      bytes.size shouldBe 94
      (bytes ++ ByteString("ignore")).iterator.decodeControlPacket() shouldBe Right(packet)
    }

    "unknown protocol name/level when decoding connect control packets" in {
      val bsb = ByteString.newBuilder
        .putByte((ControlPacketType.CONNECT.underlying << 4).toByte)
        .putByte(2)
      "blah".encode(bsb)
      bsb.putByte(0)
      bsb
        .result()
        .iterator
        .decodeControlPacket() shouldBe Left(MqttCodec.UnknownConnectProtocol(Right("blah"), 0))
    }

    "connect flag reserved set when decoding connect control packets" in {
      val bsb = ByteString.newBuilder
        .putByte((ControlPacketType.CONNECT.underlying << 4).toByte)
        .putByte(3)
      Connect.Mqtt.encode(bsb)
      bsb.putByte(Connect.v311.toByte)
      bsb.putByte(ConnectFlags.Reserved.underlying.toByte)
      bsb
        .result()
        .iterator
        .decodeControlPacket() shouldBe Left(MqttCodec.ConnectFlagReservedSet)
    }

    "bad connect message when decoding connect control packets" in {
      val bsb = ByteString.newBuilder
        .putByte((ControlPacketType.CONNECT.underlying << 4).toByte)
        .putByte(26)
      Connect.Mqtt.encode(bsb)
      bsb.putByte(Connect.v311.toByte)
      bsb.putByte(ConnectFlags.WillFlag.underlying.toByte)
      bsb.putShort(0)
      "some-client-id".encode(bsb)
      bsb
        .result()
        .iterator
        .decodeControlPacket() shouldBe Left(
        MqttCodec.BadConnectMessage(Right("some-client-id"),
                                    Some(Left(BufferUnderflow)),
                                    Some(Left(BufferUnderflow)),
                                    None,
                                    None)
      )
    }

    "encode/decode connect control ack packets" in {
      val bsb: ByteStringBuilder = ByteString.newBuilder
      val packet = ConnAck(ConnAckFlags.SessionPresent, ConnAckReturnCode.ConnectionAccepted)
      val bytes = packet.encode(bsb).result()
      bytes.size shouldBe 4
      (bytes ++ ByteString("ignore")).iterator.decodeControlPacket() shouldBe Right(packet)
    }

    "reserved bits set when decoding connect control ack packets" in {
      val bsb = ByteString.newBuilder
        .putByte((ControlPacketType.CONNACK.underlying << 4).toByte)
        .putByte(2)
      bsb.putByte(2)
      bsb.putByte(0)
      bsb
        .result()
        .iterator
        .decodeControlPacket() shouldBe Left(MqttCodec.ConnectAckFlagReservedBitsSet)
    }

    "encode/decode publish packets" in {
      val bsb: ByteStringBuilder = ByteString.newBuilder
      val packet = Publish(
        ControlPacketFlags.RETAIN | ControlPacketFlags.QoSAtLeastOnceDelivery | ControlPacketFlags.DUP,
        "some-topic-name",
        Some(PacketId(1)),
        ByteString("some-payload")
      )
      val bytes = packet.encode(bsb).result()
      bytes.size shouldBe 33
      (bytes ++ ByteString("ignore")).iterator.decodeControlPacket() shouldBe Right(packet)
    }

    "encode/decode publish packets with at most once QoS" in {
      val bsb: ByteStringBuilder = ByteString.newBuilder
      val packet = Publish("some-topic-name", ByteString("some-payload"))
      val bytes = packet.encode(bsb).result()
      bytes.size shouldBe 31
      (bytes ++ ByteString("ignore")).iterator.decodeControlPacket() shouldBe Right(packet)
    }

    "invalid QoS when decoding publish packets" in {
      val bsb = ByteString.newBuilder
        .putByte((ControlPacketType.PUBLISH.underlying << 4 | ControlPacketFlags.QoSReserved.underlying).toByte)
        .putByte(0)
      bsb
        .result()
        .iterator
        .decodeControlPacket() shouldBe Left(MqttCodec.InvalidQoS)
    }

    "bad publish message when decoding publish packets" in {
      val bsb = ByteString.newBuilder
        .putByte((ControlPacketType.PUBLISH.underlying << 4).toByte)
        .putByte(0)
      bsb
        .result()
        .iterator
        .decodeControlPacket() shouldBe Left(BadPublishMessage(Left(BufferUnderflow), None, ByteString.empty))
    }
  }
}
