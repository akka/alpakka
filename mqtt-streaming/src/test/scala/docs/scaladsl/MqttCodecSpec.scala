/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package docs.scaladsl
import java.nio.ByteOrder

import akka.stream.alpakka.mqtt.streaming._
import akka.stream.alpakka.testkit.scaladsl.LogCapturing
import akka.util.{ByteString, ByteStringBuilder}

import scala.concurrent.duration._
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class MqttCodecSpec extends AnyWordSpec with Matchers with LogCapturing {

  private implicit val byteOrder: ByteOrder = ByteOrder.BIG_ENDIAN
  import MqttCodec._

  private val MaxPacketSize = 100

  "the codec" should {
    "encode/decode strings" in {
      val bsb: ByteStringBuilder = ByteString.newBuilder
      val bytes = "hi".encode(bsb).result()
      bytes.iterator.getShort shouldBe 2
      bytes.iterator.decodeString() shouldBe Right("hi")
    }

    "underflow when decoding strings" in {
      ByteString.empty.iterator.decodeString() shouldBe Left(MqttCodec.BufferUnderflow)
    }

    "encode/decode reserved1 control packets" in {
      val bsb: ByteStringBuilder = ByteString.newBuilder
      val bytes = Reserved1.encode(bsb, 0).result()
      bytes.size shouldBe 2
      bytes.iterator.decodeControlPacket(MaxPacketSize) shouldBe Right(Reserved1)
    }

    "encode/decode reserved2 control packets" in {
      val bsb: ByteStringBuilder = ByteString.newBuilder
      val bytes = Reserved2.encode(bsb, 0).result()
      bytes.size shouldBe 2
      bytes.iterator.decodeControlPacket(MaxPacketSize) shouldBe Right(Reserved2)
    }

    "underflow when decoding control packets" in {
      ByteString.empty.iterator.decodeControlPacket(MaxPacketSize) shouldBe Left(MqttCodec.BufferUnderflow)
    }

    "invalid packet size when decoding control packets" in {
      ByteString.newBuilder
        .putByte(0x00)
        .putByte(0x01)
        .result()
        .iterator
        .decodeControlPacket(0) shouldBe Left(MqttCodec.InvalidPacketSize(1, 0))
    }

    "unknown packet type/flags when decoding control packets" in {
      ByteString.newBuilder
        .putByte(0x01)
        .putByte(0x00)
        .result()
        .iterator
        .decodeControlPacket(MaxPacketSize) shouldBe Left(
        UnknownPacketType(ControlPacketType(0), ControlPacketFlags(1))
      )
    }

    "encode/decode one byte remaining length" in {
      val bsb: ByteStringBuilder = ByteString.newBuilder
      val remainingLength = 64
      val bytes = remainingLength.encode(bsb).result()
      bytes.size shouldBe 1
      bytes.iterator.decodeRemainingLength() shouldBe Right(remainingLength)
    }

    "encode/decode two byte remaining length" in {
      val bsb: ByteStringBuilder = ByteString.newBuilder
      val remainingLength = 321
      val bytes = remainingLength.encode(bsb).result()
      bytes.size shouldBe 2
      bytes.iterator.decodeRemainingLength() shouldBe Right(remainingLength)
    }

    "encode/decode three byte remaining length" in {
      val bsb: ByteStringBuilder = ByteString.newBuilder
      val remainingLength = 16384
      val bytes = remainingLength.encode(bsb).result()
      bytes.size shouldBe 3
      bytes.iterator.decodeRemainingLength() shouldBe Right(remainingLength)
    }

    "encode/decode four byte remaining length" in {
      val bsb: ByteStringBuilder = ByteString.newBuilder
      val remainingLength = 2097152
      val bytes = remainingLength.encode(bsb).result()
      bytes.size shouldBe 4
      bytes.iterator.decodeRemainingLength() shouldBe Right(remainingLength)
    }

    "underflow when decoding remaining length" in {
      ByteString.newBuilder
        .putByte(0x80.toByte)
        .putByte(0x80.toByte)
        .putByte(0x80.toByte)
        .result()
        .iterator
        .decodeRemainingLength() shouldBe Left(MqttCodec.BufferUnderflow)
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
      bytes.iterator.decodeControlPacket(MaxPacketSize) shouldBe Right(packet)
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
        .decodeControlPacket(MaxPacketSize) shouldBe Left(MqttCodec.UnknownConnectProtocol(Right("blah"), 0))
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
        .decodeControlPacket(MaxPacketSize) shouldBe Left(MqttCodec.ConnectFlagReservedSet)
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
        .decodeControlPacket(MaxPacketSize) shouldBe Left(
        MqttCodec.BadConnectMessage(Right("some-client-id"),
                                    Some(Left(BufferUnderflow)),
                                    Some(Left(BufferUnderflow)),
                                    None,
                                    None)
      )
    }

    "underflow when decoding connect packets" in {
      ByteString.empty.iterator.decodeConnect() shouldBe Left(MqttCodec.BufferUnderflow)
    }

    "encode/decode connect ack packets" in {
      val bsb: ByteStringBuilder = ByteString.newBuilder
      val packet = ConnAck(ConnAckFlags.SessionPresent, ConnAckReturnCode.ConnectionAccepted)
      val bytes = packet.encode(bsb).result()
      bytes.size shouldBe 4
      bytes.iterator.decodeControlPacket(MaxPacketSize) shouldBe Right(packet)
    }

    "reserved bits set when decoding connect ack packets" in {
      val bsb = ByteString.newBuilder
        .putByte((ControlPacketType.CONNACK.underlying << 4).toByte)
        .putByte(2)
      bsb.putByte(2)
      bsb.putByte(0)
      bsb
        .result()
        .iterator
        .decodeConnAck() shouldBe Left(MqttCodec.ConnectAckFlagReservedBitsSet)
    }

    "underflow when decoding connect ack packets" in {
      ByteString.empty.iterator.decodeConnAck() shouldBe Left(MqttCodec.BufferUnderflow)
    }

    "encode/decode publish packets" in {
      val bsb: ByteStringBuilder = ByteString.newBuilder
      val packet = Publish(
        ControlPacketFlags.RETAIN | ControlPacketFlags.QoSAtMostOnceDelivery | ControlPacketFlags.DUP,
        "some-topic-name",
        ByteString("some-payload")
      )
      val bytes = packet.encode(bsb, None).result()
      bytes.size shouldBe 31
      bytes.iterator.decodeControlPacket(MaxPacketSize) shouldBe Right(packet)
    }

    "encode/decode publish packets with at least once QoS" in {
      val bsb: ByteStringBuilder = ByteString.newBuilder
      val packet = Publish("some-topic-name", ByteString("some-payload"))
      val bytes = packet.encode(bsb, Some(PacketId(0))).result()
      bytes.size shouldBe 33
      bytes.iterator.decodeControlPacket(MaxPacketSize) shouldBe Right(packet)
    }

    "invalid QoS when decoding publish packets" in {
      val bsb = ByteString.newBuilder
        .putByte((ControlPacketType.PUBLISH.underlying << 4 | ControlPacketFlags.QoSReserved.underlying).toByte)
        .putByte(0)
      bsb
        .result()
        .iterator
        .decodeControlPacket(MaxPacketSize) shouldBe Left(MqttCodec.InvalidQoS)
    }

    "bad publish message when decoding publish packets" in {
      val bsb = ByteString.newBuilder
        .putByte((ControlPacketType.PUBLISH.underlying << 4).toByte)
        .putByte(0)
      bsb
        .result()
        .iterator
        .decodeControlPacket(MaxPacketSize) shouldBe Left(
        BadPublishMessage(Left(BufferUnderflow), None, ByteString.empty)
      )
    }

    "underflow when decoding publish packets" in {
      val bsb = ByteString.newBuilder
      "some-topic".encode(bsb)
      bsb
        .result()
        .iterator
        .decodePublish(0, ControlPacketFlags.QoSAtLeastOnceDelivery) shouldBe Left(MqttCodec.BufferUnderflow)
    }

    "encode/decode publish ack packets" in {
      val bsb: ByteStringBuilder = ByteString.newBuilder
      val packet = PubAck(PacketId(1))
      val bytes = packet.encode(bsb).result()
      bytes.size shouldBe 4
      bytes.iterator.decodeControlPacket(MaxPacketSize) shouldBe Right(packet)
    }

    "underflow when decoding publish ack packets" in {
      ByteString.empty.iterator.decodePubAck() shouldBe Left(MqttCodec.BufferUnderflow)
    }

    "encode/decode publish rec packets" in {
      val bsb: ByteStringBuilder = ByteString.newBuilder
      val packet = PubRec(PacketId(1))
      val bytes = packet.encode(bsb).result()
      bytes.size shouldBe 4
      bytes.iterator.decodeControlPacket(MaxPacketSize) shouldBe Right(packet)
    }

    "underflow when decoding publish rec packets" in {
      ByteString.empty.iterator.decodePubRec() shouldBe Left(MqttCodec.BufferUnderflow)
    }

    "encode/decode publish rel packets" in {
      val bsb: ByteStringBuilder = ByteString.newBuilder
      val packet = PubRel(PacketId(1))
      val bytes = packet.encode(bsb).result()
      bytes.size shouldBe 4
      bytes.iterator.decodeControlPacket(MaxPacketSize) shouldBe Right(packet)
    }

    "underflow when decoding publish rel packets" in {
      ByteString.empty.iterator.decodePubRel() shouldBe Left(MqttCodec.BufferUnderflow)
    }

    "encode/decode publish comp packets" in {
      val bsb: ByteStringBuilder = ByteString.newBuilder
      val packet = PubComp(PacketId(1))
      val bytes = packet.encode(bsb).result()
      bytes.size shouldBe 4
      bytes.iterator.decodeControlPacket(MaxPacketSize) shouldBe Right(packet)
    }

    "underflow when decoding publish comp packets" in {
      ByteString.empty.iterator.decodePubComp() shouldBe Left(MqttCodec.BufferUnderflow)
    }

    "encode/decode subscribe packets" in {
      val bsb: ByteStringBuilder = ByteString.newBuilder
      val packet = Subscribe(
        List("some-head-topic" -> ControlPacketFlags.QoSExactlyOnceDelivery,
             "some-tail-topic" -> ControlPacketFlags.QoSExactlyOnceDelivery)
      )
      val bytes = packet.encode(bsb, PacketId(0)).result()
      bytes.size shouldBe 40
      bytes.iterator.decodeControlPacket(MaxPacketSize) shouldBe Right(packet)
    }

    "encode/decode subscribe packets with at least once QoS" in {
      val bsb: ByteStringBuilder = ByteString.newBuilder
      val packet = Subscribe("some-head-topic")
      val bytes = packet.encode(bsb, PacketId(0)).result()
      bytes.size shouldBe 22
      bytes.iterator.decodeControlPacket(MaxPacketSize) shouldBe Right(packet)
    }

    "bad subscribe message when decoding subscribe packets given bad QoS" in {
      val bsb: ByteStringBuilder = ByteString.newBuilder
      val packet = Subscribe(
        List("some-head-topic" -> ControlPacketFlags.QoSExactlyOnceDelivery,
             "some-tail-topic" -> ControlPacketFlags.QoSReserved)
      )
      val bytes = packet.encode(bsb, PacketId(1)).result()
      bytes.iterator
        .decodeControlPacket(MaxPacketSize) shouldBe Left(
        BadSubscribeMessage(PacketId(1),
                            List(Right("some-head-topic") -> ControlPacketFlags.QoSExactlyOnceDelivery,
                                 Right("some-tail-topic") -> ControlPacketFlags.QoSReserved))
      )
    }

    "bad subscribe message when decoding subscribe packets given no topics" in {
      val bsb: ByteStringBuilder = ByteString.newBuilder
      val packet = Subscribe(List.empty)
      val bytes = packet.encode(bsb, PacketId(1)).result()
      bytes.iterator
        .decodeControlPacket(MaxPacketSize) shouldBe Left(
        BadSubscribeMessage(PacketId(1), List.empty)
      )
    }

    "underflow when decoding subscribe packets" in {
      ByteString.empty.iterator.decodeSubscribe(0) shouldBe Left(MqttCodec.BufferUnderflow)
    }

    "encode/decode sub ack packets" in {
      val bsb: ByteStringBuilder = ByteString.newBuilder
      val packet =
        SubAck(PacketId(1), List(ControlPacketFlags.QoSExactlyOnceDelivery, ControlPacketFlags.QoSExactlyOnceDelivery))
      val bytes = packet.encode(bsb).result()
      bytes.size shouldBe 6
      bytes.iterator.decodeControlPacket(MaxPacketSize) shouldBe Right(packet)
    }

    "regular sub ack message when decoding sub ack packets given failure QoS" in {
      val bsb: ByteStringBuilder = ByteString.newBuilder
      val packet = SubAck(PacketId(1), List(ControlPacketFlags.QoSExactlyOnceDelivery, ControlPacketFlags.QoSFailure))
      val bytes = packet.encode(bsb).result()
      bytes.iterator
        .decodeControlPacket(MaxPacketSize) shouldBe Right(
        SubAck(PacketId(1), List(ControlPacketFlags.QoSExactlyOnceDelivery, ControlPacketFlags.QoSFailure))
      )
    }

    "underflow when decoding sub ack packets" in {
      ByteString.empty.iterator.decodeSubAck(0) shouldBe Left(MqttCodec.BufferUnderflow)
    }

    "encode/decode unsubscribe packets" in {
      val bsb: ByteStringBuilder = ByteString.newBuilder
      val packet = Unsubscribe("some-head-topic")
      val bytes = packet.encode(bsb, PacketId(0)).result()
      bytes.size shouldBe 21
      bytes.iterator.decodeControlPacket(MaxPacketSize) shouldBe Right(packet)
    }

    "bad unsubscribe message when decoding unsubscribe packets given no topics" in {
      val bsb: ByteStringBuilder = ByteString.newBuilder
      val packet = Unsubscribe(List.empty)
      val bytes = packet.encode(bsb, PacketId(1)).result()
      bytes.iterator
        .decodeControlPacket(MaxPacketSize) shouldBe Left(
        BadUnsubscribeMessage(PacketId(1), List.empty)
      )
    }

    "underflow when decoding unsubscribe packets" in {
      ByteString.empty.iterator.decodeUnsubscribe(0) shouldBe Left(MqttCodec.BufferUnderflow)
    }

    "encode/decode unsubscribe ack packets" in {
      val bsb: ByteStringBuilder = ByteString.newBuilder
      val packet = UnsubAck(PacketId(1))
      val bytes = packet.encode(bsb).result()
      bytes.size shouldBe 4
      bytes.iterator.decodeControlPacket(MaxPacketSize) shouldBe Right(packet)
    }

    "underflow when decoding unsubscribe ack packets" in {
      ByteString.empty.iterator.decodeUnsubAck() shouldBe Left(MqttCodec.BufferUnderflow)
    }

    "encode/decode ping req control packets" in {
      val bsb: ByteStringBuilder = ByteString.newBuilder
      val bytes = PingReq.encode(bsb).result()
      bytes.size shouldBe 2
      bytes.iterator.decodeControlPacket(MaxPacketSize) shouldBe Right(PingReq)
    }

    "encode/decode ping resp control packets" in {
      val bsb: ByteStringBuilder = ByteString.newBuilder
      val bytes = PingResp.encode(bsb).result()
      bytes.size shouldBe 2
      bytes.iterator.decodeControlPacket(MaxPacketSize) shouldBe Right(PingResp)
    }

    "encode/decode disconnect control packets" in {
      val bsb: ByteStringBuilder = ByteString.newBuilder
      val bytes = Disconnect.encode(bsb).result()
      bytes.size shouldBe 2
      bytes.iterator.decodeControlPacket(MaxPacketSize) shouldBe Right(Disconnect)
    }
  }
}
