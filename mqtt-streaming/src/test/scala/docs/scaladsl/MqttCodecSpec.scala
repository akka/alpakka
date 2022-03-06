/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.scaladsl
import java.nio.ByteOrder

import akka.stream.alpakka.mqtt.streaming._
import akka.stream.alpakka.testkit.scaladsl.LogCapturing
import akka.util.{ByteString, ByteStringBuilder}

import scala.concurrent.duration._
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

// TODO Following Packet-Tests are not yet updated
//        PUBREC
//        PUBREL
//        PUBCOMP
//        UNSUBSCRIBE
//        UNSUBACK


class MqttCodecSpec extends AnyWordSpec with Matchers with LogCapturing {

  private implicit val byteOrder: ByteOrder = ByteOrder.BIG_ENDIAN
  import MqttCodec._

  private val MaxPacketSize = 500

  "the codec" should {

    // Data Representation

    "encode/decode strings" in {
      val bsb: ByteStringBuilder = ByteString.newBuilder
      val bytes = "hi".encode(bsb).result()
      bytes.iterator.getShort shouldBe 2
      bytes.iterator.decodeString() shouldBe Right("hi")
    }

    "underflow when decoding strings" in {
      ByteString.empty.iterator.decodeString() shouldBe Left(MqttCodec.BufferUnderflow)
    }

    "encode/decode reserved control packets" in {

      val protocolLevel: Connect.ProtocolLevel = Connect.v5

      val bsb: ByteStringBuilder = ByteString.newBuilder
      val bytes = Reserved.encode(bsb, 0).result()
      bytes.size shouldBe 2
      bytes.iterator.decodeControlPacket(MaxPacketSize, protocolLevel) shouldBe Right(Reserved)
    }

    "underflow when decoding control packets" in {

      val protocolLevel: Connect.ProtocolLevel = Connect.v5

      ByteString.empty.iterator.decodeControlPacket(MaxPacketSize, protocolLevel) shouldBe Left(MqttCodec.BufferUnderflow)
    }

    "encode/decode one byte Variable Byte Integer" in {
      val bsb: ByteStringBuilder = ByteString.newBuilder
      val variableByteInt = VariableByteInteger(64)
      val bytes = variableByteInt.encode(bsb).result()
      bytes.size shouldBe 1
      bytes.iterator.decodeVariableByteInteger() shouldBe Right(variableByteInt)
    }

    "encode/decode two byte Variable Byte Integer" in {
      val bsb: ByteStringBuilder = ByteString.newBuilder
      val variableByteInt = VariableByteInteger(321)
      val bytes = variableByteInt.encode(bsb).result()
      bytes.size shouldBe 2
      bytes.iterator.decodeVariableByteInteger() shouldBe Right(variableByteInt)
    }

    "encode/decode three byte Variable Byte Integer" in {
      val bsb: ByteStringBuilder = ByteString.newBuilder
      val variableByteInt = VariableByteInteger(16384)
      val bytes = variableByteInt.encode(bsb).result()
      bytes.size shouldBe 3
      bytes.iterator.decodeVariableByteInteger() shouldBe Right(variableByteInt)
    }

    "encode/decode four byte Variable Byte Integer" in {
      val bsb: ByteStringBuilder = ByteString.newBuilder
      val variableByteInt = VariableByteInteger(2097152)
      val bytes = variableByteInt.encode(bsb).result()
      bytes.size shouldBe 4
      bytes.iterator.decodeVariableByteInteger() shouldBe Right(variableByteInt)
    }

    "underflow when decoding Variable Byte Integer" in {
      ByteString.newBuilder
        .putByte(0x80.toByte)
        .putByte(0x80.toByte)
        .putByte(0x80.toByte)
        .result()
        .iterator
        .decodeVariableByteInteger() shouldBe Left(BufferUnderflow)
    }


    // Control Packet

    "invalid packet size when decoding control packets" in {

      val protocolLevel: Connect.ProtocolLevel = Connect.v5

      ByteString.newBuilder
        .putByte(0x00)
        .putByte(0x01)
        .result()
        .iterator
        .decodeControlPacket(0, protocolLevel) shouldBe Left(MqttCodec.InvalidPacketSize(packetSize=1, maxPacketSize=0))
    }

    "unknown packet type/flags when decoding control packets" in {

      val protocolLevel: Connect.ProtocolLevel = Connect.v5

      ByteString.newBuilder
        .putByte(0x01)
        .putByte(0x00)
        .result()
        .iterator
        .decodeControlPacket(MaxPacketSize, protocolLevel) shouldBe Left(
        UnknownPacketType(ControlPacketType(0), ControlPacketFlags(1))
      )
    }


    // CONNECT Control Packet

    "unknown protocol name/level when decoding connect control packets" in {

      val protocolLevel: Connect.ProtocolLevel = Connect.v5

      val bsb = ByteString.newBuilder
        .putByte((ControlPacketType.CONNECT.underlying << 4).toByte)
        .putByte(2)
      "blah".encode(bsb)
      bsb.putByte(0)
      bsb
        .result()
        .iterator
        .decodeControlPacket(MaxPacketSize, protocolLevel) shouldBe Left(UnknownConnectProtocol(Right("blah"), 0))
    }

    "connect flag reserved set when decoding connect control packets" in {

      val protocolLevel: Connect.ProtocolLevel = Connect.v5

      val bsb = ByteString.newBuilder
        .putByte((ControlPacketType.CONNECT.underlying << 4).toByte)
        .putByte(3)
      Connect.Mqtt.encode(bsb)
      bsb.putByte(Connect.v5.toByte)
      bsb.putByte(ConnectFlags.Reserved.underlying.toByte)
      bsb
        .result()
        .iterator
        .decodeControlPacket(MaxPacketSize, protocolLevel) shouldBe Left(ConnectFlagReservedSet)
    }

    "bad connect message when decoding connect control packets" in {

      val protocolLevel: Connect.ProtocolLevel = Connect.v5

      val bsb = ByteString.newBuilder
        .putByte((ControlPacketType.CONNECT.underlying << 4).toByte)
        .putByte(26)
      Connect.Mqtt.encode(bsb)
      bsb.putByte(Connect.v5.toByte)
      bsb.putByte(ConnectFlags.WillFlag.underlying.toByte)
      bsb.putShort(0)
      "some-client-id".encode(bsb)
      bsb
        .result()
        .iterator
        .decodeControlPacket(MaxPacketSize, protocolLevel) shouldBe Left(
        BadConnectMessage(BadConnectPayload(BufferUnderflow))
      )
    }

    "underflow when decoding connect packets" in {
      ByteString.empty.iterator.decodeConnect() shouldBe Left(BadConnectMessage(BufferUnderflow))
    }

    "fail to encode Connect packets with options unavailable in MQTT v311" in {

      val protocolLevel: Connect.ProtocolLevel = Connect.v311

      val bsb: ByteStringBuilder = ByteString.newBuilder
      val props = ConnectProperties(SessionExpiryInterval=Some(30))
      val packet = Connect(
        clientId = "some-client-id",
        protocolVersion = protocolLevel,
        connectProperties = props
      )

      an [Exception] should be thrownBy
        packet.encode(bsb, protocolLevel).result()
    }

    "encode/decode connect control packets with protocol version v311" in {

      val protocolLevel: Connect.ProtocolLevel = Connect.v311

      // Test 1: Check if encoding A Connect Package yields the expected Byte Sequence. Also check
      //         if decoding that sequence yields the same Connect Package.
      val bsb1: ByteStringBuilder = ByteString.newBuilder
      val packet1 = Connect(
        clientId = "some-client-id",
        protocolVersion = protocolLevel,
        connectProperties = ConnectProperties(),
        keepAlive = 30.seconds
      )
      val bytes1 = packet1.encode(bsb1, protocolLevel).result()
      // Check if code serializes Connect Class to correct sequence of Bytes
      bytes1 shouldBe
        //--Fixed Header--//
        Seq(
          0x10,  // Binary value: |             0 0 0 1 | 0 0 0 0  |
          //               | Control Packet Type | Reserved |
          0x1a,  // Remaining Length
        ) ++
          //--Variable Header--//
          Seq(0x00, 0x04) ++ ByteString("MQTT") ++ // Protocol Name
          Seq(0x04) ++                             // Protocol Level
          Seq(0x00) ++                             // Connect Flags
          Seq(0x00, 0x1e) ++                       // Keep Alive Bytes
          Seq(0x00, 0x0e) ++ ByteString("some-client-id")    // Client Identifier

      bytes1.iterator.decodeControlPacket(MaxPacketSize, protocolLevel) shouldBe Right(packet1)


      // Test2: Check if decoding an encoded Connect Package yields the same Connect Package
      val bsb2: ByteStringBuilder = ByteString.newBuilder
      val props2 = ConnectProperties()
      val payloadWillProps2 = ConnectPayloadWillProperties()
      val lastWillMsg2 = LastWillMessage(
        willProperties = payloadWillProps2,
        willTopic = "some-will-topic",
        willPayload = BinaryData(ByteString("some-will-payload"))
      )
      val packet2 = Connect(
        clientId = "some-client-id",
        protocolVersion = protocolLevel,
        connectProperties = props2,
        username = Some("some-username"),
        password = Some("some-password"),
        keepAlive = 30.seconds,
        cleanStart = true,
        willRetain = true,
        willQoS = 1,
        lastWillMessage = Some(lastWillMsg2)
      )
      val bytes = packet2.encode(bsb2, protocolLevel).result()
      bytes.size shouldBe 94
      bytes.iterator.decodeControlPacket(MaxPacketSize, protocolLevel) shouldBe Right(packet2)

    }

    "encode/decode connect control packets with protocol version v5" in {

      val protocolLevel: Connect.ProtocolLevel = Connect.v5

      // Test 1: Check if encoding A Connect Package yields the expected Byte Sequence. Also check
      //         if decoding that sequence yields the same Connect Package.
      val bsb1: ByteStringBuilder = ByteString.newBuilder
      val packet1 = Connect(
        clientId = "some-client-id",
        protocolVersion = protocolLevel,
        connectProperties = ConnectProperties(),
        keepAlive = 30.seconds
      )
      val bytes1 = packet1.encode(bsb1, protocolLevel).result()
      // Check if code serializes Connect Class to correct sequence of Bytes
      bytes1 shouldBe
        //--Fixed Header--//
        Seq(
          0x10,  // Binary value: |             0 0 0 1 | 0 0 0 0  |
          //               | Control Packet Type | Reserved |
          0x1b,  // Remaining Length
        ) ++
          //--Variable Header--//
          Seq(0x00, 0x04) ++ ByteString("MQTT") ++ // Protocol Name
          Seq(0x05) ++                             // Protocol Level
          Seq(0x00) ++                             // Connect Flags
          Seq(0x00, 0x1e) ++                       // Keep Alive Bytes
          Seq(0x00) ++                             // Properties Length
          Seq(0x00, 0x0e) ++ ByteString("some-client-id") // Client Identifier

      bytes1.iterator.decodeControlPacket(MaxPacketSize, protocolLevel) shouldBe Right(packet1)

      // Test2: Check if decoding an encoded Connect Package yields the same Connect Package
      val bsb2: ByteStringBuilder = ByteString.newBuilder
      val props2 = ConnectProperties(
        SessionExpiryInterval = Some(1000),
        ReceiveMaximum = Some(1000),
        MaximumPacketSize = Some(32768),
        TopicAliasMaximum = Some(10),
        RequestResponseInformation = Some(0),
        RequestProblemInformation = Some(0),
        UserProperties = Some(List(("some-name-0", "some-value-0"), ("some-name-1", "some-value-1"))),
        AuthenticationMethod = Some("some-auth-method"),
        AuthenticationData = Some(BinaryData(ByteString("some-auth-data")))
      )
      val payloadWillProps2 = ConnectPayloadWillProperties(
        WillDelayInterval = Some(10),
        PayloadFormatIndicator = Some(0),
        MessageExpiryInterval = Some(30),
        ContentType = Some("some-content-type"),
        ResponseTopic = Some("some-response-topic"),
        CorrelationData = Some(BinaryData(ByteString("some-correlation-data"))),
        UserProperties = Some(List(("some-name-3", "some-value-3"), ("some-name-4", "some-value-4"))),
      )
      val lastWillMsg2 = LastWillMessage(
        willProperties = payloadWillProps2,
        willTopic = "some-will-topic",
        willPayload = BinaryData(ByteString("some-will-payload"))
      )
      val packet2 = Connect(
        clientId = "some-client-id",
        protocolVersion = protocolLevel,
        connectProperties = props2,
        username = Some("some-username"),
        password = Some("some-password"),
        keepAlive = 30.seconds,
        cleanStart = true,
        willRetain = true,
        willQoS = 1,
        lastWillMessage = Some(lastWillMsg2)
      )
      val bytes = packet2.encode(bsb2, protocolLevel).result()
      bytes.size shouldBe 344
      bytes.iterator.decodeControlPacket(MaxPacketSize, protocolLevel) shouldBe Right(packet2)

    }


    // CONNACK Control Packet

    "reserved bits set when decoding connect ack packets" in {

      val protocolLevel: Connect.ProtocolLevel = Connect.v5

      val bsb = ByteString.newBuilder
        .putByte((ControlPacketType.CONNACK.underlying << 4).toByte)
        .putByte(2)
      bsb.putByte(2)
      bsb.putByte(0)
      bsb
        .result()
        .iterator
        .decodeConnAck(protocolLevel) shouldBe Left(MqttCodec.ConnectAckFlagReservedBitsSet)
    }

    "underflow when decoding connect ack packets" in {

      val protocolLevel: Connect.ProtocolLevel = Connect.v5

      ByteString.empty.iterator.decodeConnAck(protocolLevel) shouldBe Left(BadConnAckMessage(BufferUnderflow))
    }

    "fail to encode Connect Acknowledge packets with options unavailable in MQTT v311" in {

      val protocolLevel: Connect.ProtocolLevel = Connect.v311

      val bsb: ByteStringBuilder = ByteString.newBuilder
      val props = ConnAckProperties(ReasonString=Some("some-reason-string"))
      val packet = ConnAck(ConnAckFlags.None, ConnAckReasonCode.Success, props)

      an [Exception] should be thrownBy
        packet.encode(bsb, protocolLevel).result()
    }

    "encode/decode connect ack packets with protocol version v311" in {

      val protocolLevel: Connect.ProtocolLevel = Connect.v311

      // Check if encoding A ConnAck Package yields the expected Byte Sequence. Also check
      // if decoding that sequence yields the same ConnAck Package.
      val bsb: ByteStringBuilder = ByteString.newBuilder
      val props = ConnAckProperties()
      val packet = ConnAck(ConnAckFlags.SessionPresent, ConnAckReasonCode.Success, props)
      val bytes = packet.encode(bsb, protocolLevel).result()

      bytes shouldBe
        //--Fixed Header--//
        Seq(
          0x20,  // Binary value: |             0 0 1 0 | 0 0 0 0  |
          //               | Control Packet Type | Reserved |
          0x02,  // Remaining Length
        ) ++
          //--Variable Header--//
          Seq(0x01) ++ // Connect Acknowledge Flags
          Seq(0x00)    // Connect Return Code

      bytes.iterator.decodeControlPacket(MaxPacketSize, protocolLevel) shouldBe Right(packet)

    }

    "encode/decode connect ack packets with protocol version v5" in {

      val protocolLevel: Connect.ProtocolLevel = Connect.v5

      // Test 1: Check if encoding A ConnAck Package yields the expected Byte Sequence. Also check
      //         if decoding that sequence yields the same ConnAck Package.
      val bsb1: ByteStringBuilder = ByteString.newBuilder
      val props1 = ConnAckProperties()
      val packet1 = ConnAck(ConnAckFlags.SessionPresent, ConnAckReasonCode.Success, props1)
      val bytes1 = packet1.encode(bsb1, protocolLevel).result()
      bytes1 shouldBe
        //--Fixed Header--//
        Seq(
          0x20,  // Binary value: |             0 0 1 0 | 0 0 0 0  |
          //               | Control Packet Type | Reserved |
          0x03,  // Remaining Length
        ) ++
          //--Variable Header--//
          Seq(0x01) ++ // Connect Acknowledge Flags
          Seq(0x00) ++ // Connect Reason Code
          Seq(0x00)    // Properties Length

      bytes1.iterator.decodeControlPacket(MaxPacketSize, protocolLevel) shouldBe Right(packet1)

      // Test2: Check if decoding an encoded ConnAck Package yields the same ConnAck Package.
      val bsb2: ByteStringBuilder = ByteString.newBuilder
      val props2 = ConnAckProperties(
        SessionExpiryInterval = Some(29),
        ReceiveMaximum = Some(100),
        MaximumQoS = Some(1),
        RetainAvailable = Some(1),
        MaximumPacketSize = Some(1024),
        AssignedClientIdentifier = Some("some-client-id"),
        TopicAliasMaximum = Some(100),
        ReasonString = Some("some-reason-string"),
        UserProperties = Some(List(("some-name-0", "some-value-0"), ("some-name-1", "some-value-1"))),
        WildcardSubscriptionAvailable = Some(1),
        SharedSubscriptionAvailable = Some(1),
        SubscriptionIdentifierAvailable = Some(1),
        ServerKeepAlive = Some(120),
        ResponseInformation = Some("some-response-information"),
        ServerReference = Some("some-server-reference"),
        AuthenticationMethod = Some("some-auth-method"),
        AuthenticationData = Some(BinaryData(ByteString("some-auth-data")))
      )
      val packet2 = ConnAck(ConnAckFlags.SessionPresent, ConnAckReasonCode.Success, props2)
      val bytes2 = packet2.encode(bsb2, protocolLevel).result()
      bytes2.size shouldBe 218
      bytes2.iterator.decodeControlPacket(MaxPacketSize, protocolLevel) shouldBe Right(packet2)
    }


    // PUBLISH Control Packet

    "underflow when decoding publish packets" in {

      val protocolLevel: Connect.ProtocolLevel = Connect.v5

      val bsb = ByteString.newBuilder
      "some-topic".encode(bsb)
      bsb
        .result()
        .iterator
        .decodePublish(0, ControlPacketFlags.QoSAtLeastOnceDelivery, protocolLevel) shouldBe Left(BadPublishMessage(BufferUnderflow))
    }

    "invalid QoS when decoding publish packets" in {

      val protocolLevel: Connect.ProtocolLevel = Connect.v5

      val bsb = ByteString.newBuilder
        .putByte((ControlPacketType.PUBLISH.underlying << 4 | ControlPacketFlags.QoSReserved.underlying).toByte)
        .putByte(0)
      bsb
        .result()
        .iterator
        .decodeControlPacket(MaxPacketSize, protocolLevel) shouldBe Left(MqttCodec.InvalidQoSFlag)
    }

    "bad publish message when decoding publish packets" in {

      val protocolLevel: Connect.ProtocolLevel = Connect.v5

      val bsb = ByteString.newBuilder
        .putByte((ControlPacketType.PUBLISH.underlying << 4).toByte)
        .putByte(0)
      bsb
        .result()
        .iterator
        .decodeControlPacket(MaxPacketSize, protocolLevel) shouldBe Left(
        BadPublishMessage(BufferUnderflow)
      )
    }

    "fail to encode publish packets with options unavailable in MQTT v311" in {

      val protocolLevel: Connect.ProtocolLevel = Connect.v311

      val bsb: ByteStringBuilder = ByteString.newBuilder
      val props = PublishProperties(ResponseTopic = Some("some-response-topic"))
      val packet = Publish(
        ControlPacketFlags.RETAIN | ControlPacketFlags.QoSAtMostOnceDelivery | ControlPacketFlags.DUP,
        "some-topic-name",
        props,
        ByteString("some-payload")
      )

      an [Exception] should be thrownBy
        packet.encode(bsb, Some(PacketId(1)), protocolLevel).result()
    }

    "encode/decode QoS 0 publish packets with protocol version v311" in {

      val protocolLevel: Connect.ProtocolLevel = Connect.v311

      // Check if encoding A Connect Package yields the expected Byte Sequence. Also check
      // if decoding that sequence yields the same Connect Package.
      val bsb: ByteStringBuilder = ByteString.newBuilder
      val packet = Publish(
        ControlPacketFlags.RETAIN | ControlPacketFlags.QoSAtMostOnceDelivery | ControlPacketFlags.DUP,
        "some-topic-name",
        ByteString("some-payload")
      )
      val bytes = packet.encode(bsb, None, protocolLevel).result()

      bytes shouldBe
        //--Fixed Header--//
        Seq(
          0x39,  // Binary value: |             0 0 1 1 | 1 0 0 1  |
          //               | Control Packet Type | Reserved |
          0x1D,  // Remaining Length
        ) ++
          //--Variable Header--//
          Seq(0x00, 0x0F) ++ ByteString("some-topic-name") ++ // Topic Name
          ByteString("some-payload") // Payload

      bytes.iterator.decodeControlPacket(MaxPacketSize, protocolLevel) shouldBe Right(packet)
    }

    "encode/decode publish QoS 0 packets with protocol version v5" in {

      val protocolLevel: Connect.ProtocolLevel = Connect.v5

      // Check if encoding a Publish Package yields the expected Byte Sequence. Also check
      // if decoding that sequence yields the same Publish Package.
      val bsb: ByteStringBuilder = ByteString.newBuilder
      val packet = Publish(
        ControlPacketFlags.RETAIN | ControlPacketFlags.QoSAtMostOnceDelivery | ControlPacketFlags.DUP,
        "some-topic-name",
        ByteString("some-payload")
      )
      val bytes = packet.encode(bsb, None, protocolLevel).result()

      bytes shouldBe
        //--Fixed Header--//
        Seq(
          0x39,  // Binary value: |             0 0 1 1 | 1 0 0 1  |
          //               | Control Packet Type | Reserved |
          0x1E,  // Remaining Length
        ) ++
          //--Variable Header--//
          Seq(0x00, 0x0F) ++ ByteString("some-topic-name") ++ // Topic Name
          Seq(0x00) ++ // Property Length
          ByteString("some-payload") // Payload

      bytes.iterator.decodeControlPacket(MaxPacketSize, protocolLevel) shouldBe Right(packet)
    }

    "encode/decode QoS 1 publish packets with protocol version v311" in {

      val protocolLevel: Connect.ProtocolLevel = Connect.v311

      // Check if encoding a Publish Package yields the expected Byte Sequence. Also check
      // if decoding that sequence yields the same Publish Package.
      val bsb: ByteStringBuilder = ByteString.newBuilder
      val packet = Publish(
        ControlPacketFlags.RETAIN | ControlPacketFlags.QoSAtLeastOnceDelivery | ControlPacketFlags.DUP,
        "some-topic-name",
        ByteString("some-payload")
      )
      val bytes = packet.encode(bsb, Some(PacketId(1)), protocolLevel).result()

      bytes shouldBe
        //--Fixed Header--//
        Seq(
          0x3B,  // Binary value: |             0 0 1 1 | 1 0 1 1  |
          //               | Control Packet Type | Reserved |
          0x1F,  // Remaining Length
        ) ++
          //--Variable Header--//
          Seq(0x00, 0x0F) ++ ByteString("some-topic-name") ++ // Topic Name
          Seq(0x00, 0x01) ++         // Packet Identifier
          ByteString("some-payload") // Payload

      bytes.iterator.decodeControlPacket(MaxPacketSize, protocolLevel) shouldBe Right(packet.copy(packetId=Some(PacketId(1))))
    }

    "encode/decode QoS 1 publish packets with protocol version 5" in {

      val protocolLevel: Connect.ProtocolLevel = Connect.v5

      val bsb: ByteStringBuilder = ByteString.newBuilder
      import scala.collection.immutable.IndexedSeq
      val props = PublishProperties(
        PayloadFormatIndicator = Some(0),
        MessageExpiryInterval = Some(10),
        TopicAlias = Some(0),
        ResponseTopic = Some("some-response-topic"),
        CorrelationData = Some(BinaryData(ByteString("some-correlation-data"): IndexedSeq[Byte])),
        UserProperties = Some(List(("some-name-0", "some-value-0"), ("some-name-1", "some-value-1"))),
        SubscriptionIdentifier = Some(5),
        ContentType = Some("some-content-type")
      )
      val packet = Publish(
        ControlPacketFlags.QoSAtLeastOnceDelivery,
        "some-topic-name",
        Some(PacketId(1)),
        props,
        ByteString("some-payload")
      )
      val bytes = packet.encode(bsb, Some(PacketId(1)), protocolLevel).result()
      bytes.size shouldBe 170
      bytes.iterator.decodeControlPacket(MaxPacketSize, protocolLevel) shouldBe Right(packet)

    }


    // PUBACK Control Packet

    "underflow when decoding publish ack packets" in {
      ByteString.empty.iterator.decodePubAck(2) shouldBe Left(BadPubAckMessage(BufferUnderflow))
    }

    "fail to encode Publish Acknowledge packets with options unavailable in MQTT v311" in {

      val protocolLevel: Connect.ProtocolLevel = Connect.v311

      val bsb: ByteStringBuilder = ByteString.newBuilder
      val props = PubAckProperties(ReasonString=Some("some-reason-string"))
      val packet = PubAck(PacketId(0), PubAckReasonCode.Success, Some(props))

      an [Exception] should be thrownBy
        packet.encode(bsb, protocolLevel).result()
    }

    "encode/decode publish acknowledge packets with protocol version v311" in {

      val protocolLevel: Connect.ProtocolLevel = Connect.v311

      // Test 1: Encode an instance of the class `PubAck` using `properties=None`.
      val bsb1: ByteStringBuilder = ByteString.newBuilder
      val packet1 = PubAck(PacketId(1), PubAckReasonCode.Success, None)
      val bytes1 = packet1.encode(bsb1, protocolLevel).result()
      bytes1 shouldBe
        //--Fixed Header--//
        Seq(
          0x40,  // Binary value: |             0 1 0 0 | 0 0 0 0  |
          //               | Control Packet Type | Reserved |
          0x02,  // Remaining Length
        ) ++
          //--Variable Header--//
          Seq(0x00, 0x01)  // Packet Identifier

      bytes1.iterator.decodeControlPacket(MaxPacketSize, protocolLevel) shouldBe Right(packet1)

      // Test 2: Encode an instance of the class `PubAck` using `properties=Some(PubAckProperties())`.
      val bsb2: ByteStringBuilder = ByteString.newBuilder
      val props2 = PubAckProperties()
      val packet2 = PubAck(PacketId(1), PubAckReasonCode.Success, Some(props2))
      val bytes2 = packet2.encode(bsb2, protocolLevel).result()
      bytes2 shouldBe
        //--Fixed Header--//
        Seq(
          0x40,  // Binary value: |             0 1 0 0 | 0 0 0 0  |
          //               | Control Packet Type | Reserved |
          0x02,  // Remaining Length
        ) ++
          //--Variable Header--//
          Seq(0x00, 0x01)  // Packet Identifier

      bytes2.iterator.decodeControlPacket(MaxPacketSize, protocolLevel) shouldBe Right(packet2.copy(properties=None))
    }

    "encode/decode publish acknowledge packets with protocol version v5" in {

      val protocolLevel: Connect.ProtocolLevel = Connect.v5

      // Test 1: Encode an instance of the class `PubAck` using `properties=None`.
      val bsb1: ByteStringBuilder = ByteString.newBuilder
      val packet1 = PubAck(PacketId(1), PubAckReasonCode.Success, None)
      val bytes1 = packet1.encode(bsb1, protocolLevel).result()
      bytes1 shouldBe
        //--Fixed Header--//
        Seq(
          0x40,  // Binary value: |             0 1 0 0 | 0 0 0 0  |
          //               | Control Packet Type | Reserved |
          0x03,  // Remaining Length
        ) ++
          //--Variable Header--//
          Seq(0x00, 0x01) ++ // Packet Identifier
          Seq(0x00) // Reason Code
      bytes1.iterator.decodeControlPacket(MaxPacketSize, protocolLevel) shouldBe Right(packet1)

      // Test 2: Encode an instance of the class `PubAck` using `properties=Some(PubAckProperties())`.
      val bsb2: ByteStringBuilder = ByteString.newBuilder
      val props2 = PubAckProperties()
      val packet2 = PubAck(PacketId(1), PubAckReasonCode.Success, Some(props2))
      val bytes2 = packet2.encode(bsb2, protocolLevel).result()
      bytes2 shouldBe
        //--Fixed Header--//
        Seq(
          0x40,  // Binary value: |             0 1 0 0 | 0 0 0 0  |
          //               | Control Packet Type | Reserved |
          0x04,  // Remaining Length
        ) ++
          //--Variable Header--//
          Seq(0x00, 0x01) ++ // Packet Identifier
          Seq(0x00) ++ // Reason Code
          Seq(0x00)    // Property Length
      bytes2.iterator.decodeControlPacket(MaxPacketSize, protocolLevel) shouldBe Right(packet2)

      // Test 3: Check if decoding an encoded PubAck Package yields the same PubAck Package.
      val bsb3: ByteStringBuilder = ByteString.newBuilder
      val props3 = PubAckProperties(
        Some("some-reason-string"),
        Some(List(("some-name-0", "some-value-0"), ("some-name-1", "some-value-1")))
      )
      val packet3 = PubAck(PacketId(1), PubAckReasonCode.Success, Some(props3))
      val bytes3 = packet3.encode(bsb3, protocolLevel).result()
      bytes3.size shouldBe 83
      bytes3.iterator.decodeControlPacket(MaxPacketSize, protocolLevel) shouldBe Right(packet3)

    }


    // TODO: Distinguish between v5 and v311
    // PUBREC Control Packet

    "encode/decode publish rec packets" in {

      val protocolLevel: Connect.ProtocolLevel = Connect.v5

      val bsb: ByteStringBuilder = ByteString.newBuilder
      val props = PubRecProperties(
        Some("some-reason-string"),
        Some(List(("some-name-0", "some-value-0"), ("some-name-1", "some-value-1")))
      )
      val packet = PubRec(PacketId(1), PubRecReasonCode.Success, Some(props))
      val bytes = packet.encode(bsb, protocolLevel).result()
      bytes.size shouldBe 83
      bytes.iterator.decodeControlPacket(MaxPacketSize, protocolLevel) shouldBe Right(packet)
    }

    "underflow when decoding publish rec packets" in {
      ByteString.empty.iterator.decodePubRec(2) shouldBe Left(BadPubRecMessage(BufferUnderflow))
    }


    // TODO: Distinguish between v5 and v311
    // PUBREL Control Packet

    "encode/decode publish rel packets" in {

      val protocolLevel: Connect.ProtocolLevel = Connect.v5

      val bsb: ByteStringBuilder = ByteString.newBuilder
      val props = PubRelProperties(
        Some("some-reason-string"),
        Some(List(("some-name-0", "some-value-0"), ("some-name-1", "some-value-1")))
      )
      val packet = PubRel(PacketId(1), PubRelReasonCode.Success, Some(props))
      val bytes = packet.encode(bsb, protocolLevel).result()
      bytes.size shouldBe 83
      bytes.iterator.decodeControlPacket(MaxPacketSize, protocolLevel) shouldBe Right(packet)
    }

    "underflow when decoding publish rel packets" in {
      ByteString.empty.iterator.decodePubRel(2) shouldBe Left(BadPubRelMessage(BufferUnderflow))
    }


    // TODO: Distinguish between v5 and v311
    // PUBCOMP Control Packet

    "encode/decode publish comp packets" in {

      val protocolLevel: Connect.ProtocolLevel = Connect.v5

      val bsb: ByteStringBuilder = ByteString.newBuilder
      val props = PubCompProperties(
        Some("some-reason-string"),
        Some(List(("some-name-0", "some-value-0"), ("some-name-1", "some-value-1")))
      )
      val packet = PubComp(PacketId(1), PubCompReasonCode.Success, Some(props))
      val bytes = packet.encode(bsb, protocolLevel).result()
      bytes.size shouldBe 83
      bytes.iterator.decodeControlPacket(MaxPacketSize, protocolLevel) shouldBe Right(packet)
    }

    "underflow when decoding publish comp packets" in {
      ByteString.empty.iterator.decodePubComp(2) shouldBe Left(BadPubCompMessage(BufferUnderflow))
    }


    // SUBSCRIBE Control Packet

    "underflow when decoding subscribe packets" in {
      val protocolLevel: Connect.ProtocolLevel = Connect.v5

      ByteString.empty.iterator.decodeSubscribe(protocolLevel) shouldBe Left(BadSubscribeMessage(BufferUnderflow))
    }

    "bad subscribe message when decoding subscribe packets given bad QoS" in {

      val protocolLevel: Connect.ProtocolLevel = Connect.v5

      val bsb: ByteStringBuilder = ByteString.newBuilder
      val packet = Subscribe(
        Seq("some-head-topic" -> SubscribeOptions.QoSExactlyOnceDelivery,
          "some-tail-topic" -> SubscribeOptions.QoSReserved)
      )
      val bytes = packet.encode(bsb, PacketId(1), protocolLevel).result()
      bytes.iterator
        .decodeControlPacket(MaxPacketSize, protocolLevel) shouldBe Left(
        BadSubscribeMessage(InvalidQoSFlag)
      )
    }

    "bad subscribe message when decoding subscribe packets given no topics" in {

      val protocolLevel: Connect.ProtocolLevel = Connect.v5

      val bsb: ByteStringBuilder = ByteString.newBuilder
      val packet = Subscribe(List.empty)
      val bytes = packet.encode(bsb, PacketId(1), protocolLevel).result()
      bytes.iterator
        .decodeControlPacket(MaxPacketSize, protocolLevel) shouldBe Left(
        BadSubscribeMessage(EmptyTopicFilterListNotAllowed)
      )
    }

    "fail to encode subscribe packets with options unavailable in MQTT v311" in {

      val protocolLevel: Connect.ProtocolLevel = Connect.v311

      val bsb: ByteStringBuilder = ByteString.newBuilder
      val props = SubscribeProperties(SubscriptionIdentifier = Some(5))
      val packet = Subscribe(
        props,
        Seq("some-topic/a" -> SubscribeOptions.QoSAtMostOnceDelivery)
      )

      an [Exception] should be thrownBy
        packet.encode(bsb, PacketId(1), protocolLevel).result()
    }

    "fail to encode subscribe packets with topic filter flags unavailable in MQTT v311" in {

      val protocolLevel: Connect.ProtocolLevel = Connect.v311

      // Test 1: Topic Filter Flag 'Retain Handling' (1)
      val bsb1: ByteStringBuilder = ByteString.newBuilder
      val props1 = SubscribeProperties()
      val packet1 = Subscribe(
        props1,
        Seq("some-topic/a" -> SubscribeOptions.RetainHandling1,
        )
      )

      // Test 2: Topic Filter Flag 'Retain Handling' (2)
      val bsb2: ByteStringBuilder = ByteString.newBuilder
      val props2 = SubscribeProperties()
      val packet2 = Subscribe(
        props2,
        Seq("some-topic/a" -> SubscribeOptions.RetainHandling2,
        )
      )

      an [Exception] should be thrownBy
        packet2.encode(bsb2, PacketId(1), protocolLevel).result()

      // Test 3: Topic Filter Flag 'Retain as Published (RAP)'
      val bsb3: ByteStringBuilder = ByteString.newBuilder
      val props3 = SubscribeProperties()
      val packet3 = Subscribe(
        props3,
        Seq("some-topic/a" -> SubscribeOptions.RetainAsPublished,
        )
      )

      an [Exception] should be thrownBy
        packet1.encode(bsb3, PacketId(1), protocolLevel).result()

      // Test 4: Topic Filter Flag 'No Local (NL)'
      val bsb4: ByteStringBuilder = ByteString.newBuilder
      val props4 = SubscribeProperties()
      val packet4 = Subscribe(
        props4,
        Seq("some-topic/a" -> SubscribeOptions.NoLocal,
        )
      )

      an [Exception] should be thrownBy
        packet4.encode(bsb4, PacketId(1), protocolLevel).result()
    }

    "encode/decode subscribe packets with protocol level v311" in {

      val protocolLevel: Connect.ProtocolLevel = Connect.v311

      val bsb: ByteStringBuilder = ByteString.newBuilder
      val props = SubscribeProperties()
      val packet = Subscribe(
        props,
        Seq("some-topic/a" -> SubscribeOptions.QoSAtMostOnceDelivery,
          "some-topic/b" -> SubscribeOptions.QoSAtLeastOnceDelivery,
          "some-topic/c" -> SubscribeOptions.QoSExactlyOnceDelivery,
        )
      )
      val bytes = packet.encode(bsb, PacketId(1), protocolLevel).result()

      bytes shouldBe
        //--Fixed Header--//
        Seq(
          0x82.toByte,
          // Binary value: |             1 0 0 0 | 0 0 1 0  |
          //               | Control Packet Type | Reserved |
          0x2F,  // Remaining Length
        ) ++
          //--Variable Header--//
          Seq(0x00, 0x01) ++ // Packet Identifier
          //---- First Subscription ---------------------------//
          Seq(0x00, 0x0C) ++            // Length
          ByteString("some-topic/a") ++ // Topic Filter
          Seq(0x00) ++                  // Requested QoS
          //---- Second Subscription ---------------------------//
          Seq(0x00, 0x0C) ++            // Length
          ByteString("some-topic/b") ++ // Topic Filter
          Seq(0x01) ++                  // Requested QoS
          //---- Third Subscription ---------------------------//
          Seq(0x00, 0x0C) ++            // Length
          ByteString("some-topic/c") ++ // Topic Filter
          Seq(0x02)                     // Requested QoS

      bytes.iterator.decodeControlPacket(MaxPacketSize, protocolLevel) shouldBe Right(packet.copy(packetId=PacketId(1)))
    }

    "encode/decode subscribe packets with protocol level v5" in {

      val protocolLevel: Connect.ProtocolLevel = Connect.v5

      // Test 1: Check if encoding a Subscribe Package yields the expected Byte Sequence. Also check
      //         if decoding that sequence yields the same Subscribe Package.
      val bsb1: ByteStringBuilder = ByteString.newBuilder
      val props1 = SubscribeProperties()
      val packet1 = Subscribe(
        props1,
        Seq("some-topic/a" -> SubscribeOptions.QoSAtMostOnceDelivery,
          "some-topic/b" -> SubscribeOptions.QoSAtLeastOnceDelivery,
          "some-topic/c" -> SubscribeOptions.QoSExactlyOnceDelivery,
        )
      )
      val bytes1 = packet1.encode(bsb1, PacketId(1), protocolLevel).result()

      bytes1 shouldBe
        //--Fixed Header--//
        Seq(
          0x82.toByte,
          // Binary value: |             1 0 0 0 | 0 0 1 0  |
          //               | Control Packet Type | Reserved |
          0x30,  // Remaining Length
        ) ++
          //--Variable Header--//
          Seq(0x00, 0x01) ++ // Packet Identifier
          Seq(0x00) ++       // Property Length
          //---- First Subscription ---------------------------//
          Seq(0x00, 0x0C) ++            // Length
          ByteString("some-topic/a") ++ // Topic Filter
          Seq(0x00) ++                  // Requested QoS
          //---- Second Subscription ---------------------------//
          Seq(0x00, 0x0C) ++            // Length
          ByteString("some-topic/b") ++ // Topic Filter
          Seq(0x01) ++                  // Requested QoS
          //---- Third Subscription ---------------------------//
          Seq(0x00, 0x0C) ++            // Length
          ByteString("some-topic/c") ++ // Topic Filter
          Seq(0x02)                     // Requested QoS

      bytes1.iterator.decodeControlPacket(MaxPacketSize, protocolLevel) shouldBe Right(packet1.copy(packetId=PacketId(1)))

      // Test 2: Check if decoding an encoded Subscribe Package yields the same Subscribe Package.
      val bsb2: ByteStringBuilder = ByteString.newBuilder
      val props2 = SubscribeProperties(
        SubscriptionIdentifier = Some(5),
        UserProperties = Some(List(("some-name-0", "some-value-0"), ("some-name-1", "some-value-1")))
      )
      val packet2 = Subscribe(
        props2,
        Seq("some-topic/a" -> SubscribeOptions.QoSAtMostOnceDelivery,
          "some-topic/b" -> SubscribeOptions.QoSAtLeastOnceDelivery,
          "some-topic/c" -> SubscribeOptions.QoSExactlyOnceDelivery,
        )
      )
      val bytes2 = packet2.encode(bsb2, PacketId(1), protocolLevel).result()
      bytes2.iterator.decodeControlPacket(MaxPacketSize, protocolLevel) shouldBe Right(packet2.copy(packetId=PacketId(1)))
    }


    // SUBACK Control Packet

    "underflow when decoding sub ack packets" in {

      val protocolLevel: Connect.ProtocolLevel = Connect.v5

      ByteString.empty.iterator.decodeSubAck(protocolLevel) shouldBe Left(BadSubAckMessage(BufferUnderflow))
    }

    "fail to encode subscribe acknowledge packets with options unavailable in MQTT v311" in {

      val protocolLevel: Connect.ProtocolLevel = Connect.v311

      val bsb: ByteStringBuilder = ByteString.newBuilder
      val props = SubAckProperties(ReasonString=Some("some-reason-string"))
      val packet = SubAck(PacketId(1), props, Seq(SubAckReasonCode.GrantedQoS1, SubAckReasonCode.GrantedQoS2))

      an [Exception] should be thrownBy
        packet.encode(bsb, protocolLevel).result()
    }

    "fail to encode subscribe acknowledge packets with return code unavailable in MQTT v311" in {

      val protocolLevel: Connect.ProtocolLevel = Connect.v311

      val bsb: ByteStringBuilder = ByteString.newBuilder
      val props = SubAckProperties()
      val packet = SubAck(PacketId(1), props, Seq(SubAckReasonCode.NotAuthorized))

      an [Exception] should be thrownBy
        packet.encode(bsb, protocolLevel).result()
    }

    "encode/decode subscribe acknowledge packets with protocol level v311" in {

      val protocolLevel: Connect.ProtocolLevel = Connect.v311

      val bsb: ByteStringBuilder = ByteString.newBuilder
      val props = SubAckProperties()
      val packet =
        SubAck(PacketId(1), props, List(SubAckReasonCode.GrantedQoS1, SubAckReasonCode.UnspecifiedError))
      val bytes = packet.encode(bsb, protocolLevel).result()

      bytes shouldBe
        //--Fixed Header--//
        (Seq(
          0x90,
          // Binary value: |             1 0 0 1 | 0 0 0 0  |
          //               | Control Packet Type | Reserved |
          0x04,  // Remaining Length
        ) ++
          //--Variable Header--//
          Seq(0x00, 0x01) ++  // Packet Identifier
          //---- First Subscription ---------------------------//
          Seq(0x01) ++  // Return Code
          //---- Second Subscription ---------------------------//
          Seq(0x80))    // Return Code
          .map(_.toByte)

      bytes.iterator.decodeControlPacket(MaxPacketSize, protocolLevel) shouldBe Right(packet)
    }

    "encode/decode subscribe acknowledge packets with protocol level v5" in {

      val protocolLevel: Connect.ProtocolLevel = Connect.v5

      // Test 1: Check if encoding a Subscribe Acknowledge Package yields the expected Byte Sequence. Also check
      //         if decoding that sequence yields the same Subscribe Acknowledge Package.
      val bsb1: ByteStringBuilder = ByteString.newBuilder
      val props1 = SubAckProperties()
      val packet1 =
        SubAck(PacketId(1), props1, List(SubAckReasonCode.GrantedQoS1, SubAckReasonCode.UnspecifiedError))
      val bytes1 = packet1.encode(bsb1, protocolLevel).result()

      bytes1 shouldBe
        //--Fixed Header--//
        (Seq(
          0x90,
          // Binary value: |             1 0 0 1 | 0 0 0 0  |
          //               | Control Packet Type | Reserved |
          0x05,  // Remaining Length
        ) ++
          //--Variable Header--//
          Seq(0x00, 0x01) ++  // Packet Identifier
          Seq(0x00) ++ // Property Length
          //--Payload--//
          //---- First Subscription ---------------------------//
          Seq(0x01) ++  // Return Code
          //---- Second Subscription ---------------------------//
          Seq(0x80))    // Return Code
          .map(_.toByte)

      bytes1.iterator.decodeControlPacket(MaxPacketSize, protocolLevel) shouldBe Right(packet1)

      // Test 2: Check if decoding an encoded Subscribe Acknowledge Package yields the same Subscribe Acknowledge Package.
      val bsb2: ByteStringBuilder = ByteString.newBuilder
      val props2 = SubAckProperties(
        ReasonString = Some("some-reason-string"),
        UserProperties = Some(List(("some-name-0", "some-value-0"), ("some-name-1", "some-value-1")))
      )
      val packet2 =
        SubAck(PacketId(1), props2, List(SubAckReasonCode.GrantedQoS1, SubAckReasonCode.GrantedQoS2))
      val bytes = packet2.encode(bsb2, protocolLevel).result()
      bytes.size shouldBe 84
      bytes.iterator.decodeControlPacket(MaxPacketSize, protocolLevel) shouldBe Right(packet2)

    }


    // TODO: Distinguish between v5 and v311
    // UNSUBSCRIBE Control Packet

    "encode/decode unsubscribe packets" in {

      val protocolLevel: Connect.ProtocolLevel = Connect.v5

      val bsb: ByteStringBuilder = ByteString.newBuilder
      val props = UnsubscribeProperties(
        UserProperties = Some(List(("some-name-0", "some-value-0"), ("some-name-1", "some-value-1")))
      )
      val packet = Unsubscribe(props, "some-head-topic")
      val bytes = packet.encode(bsb, PacketId(0), protocolLevel).result()
      bytes.size shouldBe 78
      bytes.iterator.decodeControlPacket(MaxPacketSize, protocolLevel) shouldBe Right(packet)
    }

    "bad unsubscribe message when decoding unsubscribe packets given no topics" in {

      val protocolLevel: Connect.ProtocolLevel = Connect.v5

      val bsb: ByteStringBuilder = ByteString.newBuilder
      val packet = Unsubscribe(List.empty)
      val bytes = packet.encode(bsb, PacketId(1), protocolLevel).result()
      bytes.iterator
        .decodeControlPacket(MaxPacketSize, protocolLevel) shouldBe Left(
        BadUnsubscribeMessage(EmptyTopicFilterListNotAllowed)
      )
    }

    "underflow when decoding unsubscribe packets" in {
      ByteString.empty.iterator.decodeUnsubscribe() shouldBe Left(BadUnsubscribeMessage(BufferUnderflow))
    }


    // TODO: Distinguish between v5 and v311
    // UNSUBACK Control Packet

    "encode/decode unsubscribe ack packets" in {

      val protocolLevel: Connect.ProtocolLevel = Connect.v5

      val bsb: ByteStringBuilder = ByteString.newBuilder
      val props = UnsubAckProperties(
        ReasonString = Some("some-reason-string"),
        UserProperties = Some(List(("some-name-0", "some-value-0"), ("some-name-1", "some-value-1")))
      )
      val packet = UnsubAck(PacketId(0), props,
        Seq(UnsubAckReasonCode.Success, UnsubAckReasonCode.NotAuthorized))
      val bytes = packet.encode(bsb, protocolLevel).result()
      bytes.size shouldBe 84
      bytes.iterator.decodeControlPacket(MaxPacketSize, protocolLevel) shouldBe Right(packet)
    }

    "underflow when decoding unsubscribe ack packets" in {
      ByteString.empty.iterator.decodeUnsubAck() shouldBe Left(BadUnsubAckMessage(BufferUnderflow))
    }


    // PINGREQ Control Packet

    "encode/decode ping req control packets" in {

      val protocolLevel: Connect.ProtocolLevel = Connect.v5

      val bsb: ByteStringBuilder = ByteString.newBuilder
      val bytes = PingReq.encode(bsb).result()
      bytes.size shouldBe 2
      bytes.iterator.decodeControlPacket(MaxPacketSize, protocolLevel) shouldBe Right(PingReq)
    }


    // PINGRESP Control Packet

    "encode/decode ping resp control packets" in {

      val protocolLevel: Connect.ProtocolLevel = Connect.v5

      val bsb: ByteStringBuilder = ByteString.newBuilder
      val bytes = PingResp.encode(bsb).result()
      bytes.size shouldBe 2
      bytes.iterator.decodeControlPacket(MaxPacketSize, protocolLevel) shouldBe Right(PingResp)
    }


    // DISCONNECT Control Packet

    "underflow when decoding disconnect packets" in {

      val protocolLevel: Connect.ProtocolLevel = Connect.v5

      ByteString.empty.iterator.decodeDisconnect(10) shouldBe Left(BadDisconnectMessage(BufferUnderflow))
    }

    "fail to encode disconnect control packets with options unavailable in MQTT v311" in {

      val protocolLevel: Connect.ProtocolLevel = Connect.v311

      val bsb: ByteStringBuilder = ByteString.newBuilder
      val props = DisconnectProperties(ReasonString=Some("some-reason-string"))
      val packet = Disconnect(DisconnectReasonCode.NormalDisconnection, props)

      an [Exception] should be thrownBy
        packet.encode(bsb, protocolLevel).result()
    }

    "fail to encode disconnect control packets with reason code unavailable in MQTT v311" in {

      val protocolLevel: Connect.ProtocolLevel = Connect.v311

      val bsb: ByteStringBuilder = ByteString.newBuilder
      val props = DisconnectProperties()
      val packet = Disconnect(DisconnectReasonCode.DisconnectWithWillMessage, props)

      an [Exception] should be thrownBy
        packet.encode(bsb, protocolLevel).result()
    }

    "encode/decode disconnect control packets with protocol level v311" in {

      val protocolLevel: Connect.ProtocolLevel = Connect.v311

      val bsb: ByteStringBuilder = ByteString.newBuilder
      val packet = Disconnect()
      val bytes = packet.encode(bsb, protocolLevel).result()
      bytes shouldBe
        //--Fixed Header--//
        Seq(
          0xE0,
          // Binary value: |             1 1 1 0 | 0 0 0 0  |
          //               | Control Packet Type | Reserved |
          0x00,  // Remaining Length
        ).map(_.toByte)

      bytes.iterator.decodeControlPacket(MaxPacketSize, protocolLevel) shouldBe Right(packet)

    }

    "encode/decode disconnect control packets with protocol level v5" in {

      val protocolLevel: Connect.ProtocolLevel = Connect.v5

      // Test 1: Check if encoding a Subscribe Package yields the expected Byte Sequence. Also check
      //         if decoding that sequence yields the same Subscribe Package.
      val bsb1: ByteStringBuilder = ByteString.newBuilder
      val props1 = DisconnectProperties(
        ReasonString = Some("some-reason-string"),
      )
      val packet1 = Disconnect(DisconnectReasonCode.NormalDisconnection, props1)
      val bytes1 = packet1.encode(bsb1, protocolLevel).result()

      bytes1 shouldBe
        //--Fixed Header--//
        (Seq(
          0xE0.toByte,
          // Binary value: |             1 1 1 0 | 0 0 0 0  |
          //               | Control Packet Type | Reserved |
          0x17,  // Remaining Length
        ) ++
          //--Variable Header--//
          Seq(0x00) ++ // Reason Code
          Seq(0x15) ++ // Property Length
          Seq(0x1F) ++ // Reason String Property Code
          Seq(0x00, 0x12) ++ ByteString("some-reason-string"))

      bytes1.iterator.decodeControlPacket(MaxPacketSize, protocolLevel) shouldBe Right(packet1)

      // Test 2: Check if decoding an encoded Subscribe Acknowledge Package yields the same Subscribe Acknowledge Package.
      val bsb2: ByteStringBuilder = ByteString.newBuilder
      val props2 = DisconnectProperties(
        SessionExpiryInterval = Some(50),
        ReasonString = Some("some-reason-string"),
        UserProperties = Some(List(("some-name-0", "some-value-0"), ("some-name-1", "some-value-1"))),
        ServerReference = Some("some-server-reference")
      )
      val packet2 = Disconnect(DisconnectReasonCode.NormalDisconnection, props2)
      val bytes2 = packet2.encode(bsb2, protocolLevel).result()
      bytes2.size shouldBe 110
      bytes2.iterator.decodeControlPacket(MaxPacketSize, protocolLevel) shouldBe Right(packet2)
    }


    // AUTH Control Packet

    "underflow when decoding auth packets" in {
      ByteString.empty.iterator.decodeAuth() shouldBe Left(BadAuthMessage(BufferUnderflow))
    }

    "fail to encode auth control packets when using in MQTT v311" in {

      val protocolLevel: Connect.ProtocolLevel = Connect.v311

      val bsb: ByteStringBuilder = ByteString.newBuilder
      val props = AuthProperties()
      val packet = Auth(AuthReasonCode.ContinueAuthentication, props)

      an [Exception] should be thrownBy
        packet.encode(bsb, protocolLevel).result()
    }

    "encode/decode auth control packets" in {

      val protocolLevel: Connect.ProtocolLevel = Connect.v5

      // Test 1: Check if encoding a Auth Package yields the expected Byte Sequence. Also check
      //         if decoding that sequence yields the same Auth Acknowledge Package.
      val bsb1: ByteStringBuilder = ByteString.newBuilder
      val props1 = AuthProperties(AuthenticationMethod="some-authentication-method")
      val packet1 = Auth(AuthReasonCode.ContinueAuthentication, props1)
      val bytes1 = packet1.encode(bsb1, protocolLevel).result()

      bytes1 shouldBe
        //--Fixed Header--//
        (Seq(
          0xF0.toByte,
          // Binary value: |             1 1 1 1 | 0 0 0 0  |
          //               | Control Packet Type | Reserved |
          0x1F,  // Remaining Length
        ) ++
          //--Variable Header--//
          Seq(0x18) ++ // Reason Code
          Seq(0x1D) ++ // Property Length
          Seq(0x15) ++ Seq(0x00, 0x1A) ++ ByteString("some-authentication-method"))

      bytes1.iterator.decodeControlPacket(MaxPacketSize, protocolLevel) shouldBe Right(packet1)

      // Test 2: Check if decoding an encoded Auth Package yields the same Auth Acknowledge Package.
      val bsb: ByteStringBuilder = ByteString.newBuilder
      val props = AuthProperties(
        AuthenticationMethod = "some-authentication-method",
        AuthenticationData = Some(BinaryData(ByteString("some-authentication-data"))),
        ReasonString = Some("some-reason-string"),
        UserProperties = Some(List(("some-name-0", "some-value-0"), ("some-name-1", "some-value-1")))
      )
      val packet = Auth(AuthReasonCode.ContinueAuthentication, props)
      val bytes = packet.encode(bsb, protocolLevel).result()
      bytes.size shouldBe 139
      bytes.iterator.decodeControlPacket(MaxPacketSize, protocolLevel) shouldBe Right(packet)
    }

  }
}
