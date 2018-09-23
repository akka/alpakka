/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.mqtt.streaming
import java.nio.ByteOrder
import java.nio.charset.StandardCharsets
import java.util.NoSuchElementException
import java.util.concurrent.TimeUnit

import akka.stream.alpakka.mqtt.streaming.Connect.ProtocolLevel
import akka.util.{ByteIterator, ByteString, ByteStringBuilder}

import scala.concurrent.duration._

/**
 * 2.2.1 MQTT Control Packet type
 */
object ControlPacketType {
  val Reserved1 = ControlPacketType(0)
  val CONNECT = ControlPacketType(1)
  val CONNACK = ControlPacketType(2)
  val PUBLISH = ControlPacketType(3)
  val PUBACK = ControlPacketType(4)
  val PUBREC = ControlPacketType(5)
  val PUBREL = ControlPacketType(6)
  val PUBCOMP = ControlPacketType(7)
  val SUBSCRIBE = ControlPacketType(8)
  val SUBACK = ControlPacketType(9)
  val UNSUBSCRIBE = ControlPacketType(10)
  val UNSUBACK = ControlPacketType(11)
  val PINGREQ = ControlPacketType(12)
  val PINGRESP = ControlPacketType(13)
  val DISCONNECT = ControlPacketType(14)
  val Reserved2 = ControlPacketType(15)
}
final case class ControlPacketType(underlying: Int) extends AnyVal

/**
 * 2.2.2 Flags
 */
object ControlPacketFlags {
  val None = ControlPacketFlags(0)
  val Reserved = ControlPacketFlags(0)
  val ReservedPubRel = ControlPacketFlags(1 << 1)
  val DUP = ControlPacketFlags(1 << 3)
  val QoSAtMostOnceDelivery = ControlPacketFlags(0)
  val QoSAtLeastOnceDelivery = ControlPacketFlags(1 << 1)
  val QoSExactlyOnceDelivery = ControlPacketFlags(2 << 1)
  val QoSReserved = ControlPacketFlags(3 << 1)
  val RETAIN = ControlPacketFlags(1)
}

final case class ControlPacketFlags(underlying: Int) extends AnyVal {

  /**
   * Convenience bitwise OR
   */
  def |(rhs: ControlPacketFlags): ControlPacketFlags =
    ControlPacketFlags(underlying | rhs.underlying)
}

/**
 * 2 MQTT Control Packet format
 */
sealed abstract class ControlPacket(val packetType: ControlPacketType, val flags: ControlPacketFlags)

case object Reserved1 extends ControlPacket(ControlPacketType.Reserved1, ControlPacketFlags.Reserved)

case object Reserved2 extends ControlPacket(ControlPacketType.Reserved2, ControlPacketFlags.Reserved)

object ConnectFlags {
  val None = ConnectFlags(0)
  val Reserved = ConnectFlags(1)
  val CleanSession = ConnectFlags(1 << 1)
  val WillFlag = ConnectFlags(1 << 2)
  val WillQoS = ConnectFlags(3 << 3)
  val WillRetain = ConnectFlags(1 << 5)
  val PasswordFlag = ConnectFlags(1 << 6)
  val UsernameFlag = ConnectFlags(1 << 7)
}

/**
 * 2.3.1 Packet Identifier
 */
final case class PacketId(underlying: Int) extends AnyVal

/**
 * 3.1.2.3 Connect Flags
 */
final case class ConnectFlags(underlying: Int) extends AnyVal {

  /**
   * Convenience bitwise OR
   */
  def |(rhs: ConnectFlags): ConnectFlags =
    ConnectFlags(underlying | rhs.underlying)
}

object Connect {
  type ProtocolName = String
  val Mqtt: ProtocolName = "MQTT"

  type ProtocolLevel = Int
  val v311: ProtocolLevel = 4

  /**
   * Conveniently create a connect object with credentials. This function will also set the
   * corresponding username and password flags.
   */
  def apply(clientId: String, extraConnectFlags: ConnectFlags, username: String, password: String): Connect =
    new Connect(
      Mqtt,
      v311,
      clientId,
      extraConnectFlags | ConnectFlags.UsernameFlag | ConnectFlags.PasswordFlag,
      60.seconds,
      None,
      None,
      Some(username),
      Some(password)
    )
}

/**
 * 3.1 CONNECT – Client requests a connection to a Server
 */
final case class Connect(protocolName: Connect.ProtocolName,
                         protocolLevel: Connect.ProtocolLevel,
                         clientId: String,
                         connectFlags: ConnectFlags,
                         keepAlive: FiniteDuration,
                         willTopic: Option[String],
                         willMessage: Option[String],
                         username: Option[String],
                         password: Option[String])
    extends ControlPacket(ControlPacketType.CONNECT, ControlPacketFlags.Reserved)

object ConnAckFlags {
  val None = ConnAckFlags(0)
  val SessionPresent = ConnAckFlags(1)
}

/**
 * 3.2.2.1 Connect Acknowledge Flags
 */
final case class ConnAckFlags(underlying: Int) extends AnyVal

object ConnAckReturnCode {
  val ConnectionAccepted = ConnAckReturnCode(0)
  val ConnectionRefusedUnacceptableProtocolVersion = ConnAckReturnCode(1)
  val ConnectionRefusedIdentifierRejected = ConnAckReturnCode(2)
  val ConnectionRefusedServerUnavailable = ConnAckReturnCode(3)
  val ConnectionRefusedBadUsernameOrPassword = ConnAckReturnCode(4)
  val ConnectionRefusedNotAuthorized = ConnAckReturnCode(5)
}

/**
 * 3.2.2.3 Connect Return code
 */
final case class ConnAckReturnCode(underlying: Int) extends AnyVal

/**
 * 3.2 CONNACK – Acknowledge connection request
 */
final case class ConnAck(connectAckFlags: ConnAckFlags, returnCode: ConnAckReturnCode)
    extends ControlPacket(ControlPacketType.CONNACK, ControlPacketFlags.Reserved)

object Publish {

  /**
   * Conveniently create a publish message with at most once delivery
   */
  def apply(topicName: String, payload: ByteString): Publish =
    Publish(ControlPacketFlags.None, topicName, None, payload)
}

/**
 * 3.3 PUBLISH – Publish message
 */
final case class Publish(override val flags: ControlPacketFlags,
                         topicName: String,
                         packetId: Option[PacketId],
                         payload: ByteString)
    extends ControlPacket(ControlPacketType.PUBLISH, flags)

/**
 * 3.4 PUBACK – Publish acknowledgement
 */
final case class PubAck(packetId: PacketId) extends ControlPacket(ControlPacketType.PUBACK, ControlPacketFlags.Reserved)

/**
 * 3.5 PUBREC – Publish received (QoS 2 publish received, part 1)
 */
final case class PubRec(packetId: PacketId) extends ControlPacket(ControlPacketType.PUBREC, ControlPacketFlags.Reserved)

/**
 * 3.6 PUBREL – Publish release (QoS 2 publish received, part 2)
 */
final case class PubRel(packetId: PacketId)
    extends ControlPacket(ControlPacketType.PUBREL, ControlPacketFlags.ReservedPubRel)

/**
 * Provides functions to decode bytes to various MQTT types and vice-versa.
 * Performed in accordance with http://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html
 * with section numbers referenced accordingly.
 */
object MqttCodec {

  private implicit val byteOrder: ByteOrder = ByteOrder.BIG_ENDIAN

  /**
   * Returned by decoding when no decoding can be performed
   */
  sealed abstract class DecodeStatus(val isError: Boolean)

  /**
   * Not enough bytes in the byte iterator
   */
  final case object BufferUnderflow extends DecodeStatus(isError = true)

  /**
   * Cannot determine the type/flags combination of the control packet
   */
  final case class UnknownPacketType(packetType: ControlPacketType, flags: ControlPacketFlags)
      extends DecodeStatus(isError = true)

  /**
   * Cannot determine the protocol name/level combination of the connect
   */
  final case class UnknownConnectProtocol(protocolName: Either[DecodeStatus, String], protocolLevel: ProtocolLevel)
      extends DecodeStatus(isError = true)

  /**
   * Bit 0 of the connect flag was set - which it should not be as it is reserved.
   */
  final case object ConnectFlagReservedSet extends DecodeStatus(isError = true)

  /**
   * Something is wrong with the connect message
   */
  final case class BadConnectMessage(clientId: Either[MqttCodec.DecodeStatus, String],
                                     willTopic: Option[Either[MqttCodec.DecodeStatus, String]],
                                     willMessage: Option[Either[MqttCodec.DecodeStatus, String]],
                                     username: Option[Either[MqttCodec.DecodeStatus, String]],
                                     password: Option[Either[MqttCodec.DecodeStatus, String]])
      extends DecodeStatus(isError = true)

  /**
   * A reserved QoS was specified
   */
  case object InvalidQoS extends DecodeStatus(isError = true)

  /**
   * Bits 1  to 7 are set with the Connect Ack flags
   */
  case object ConnectAckFlagReservedBitsSet extends DecodeStatus(isError = true)

  /**
   * Something is wrong with the publish message
   */
  case class BadPublishMessage(topicName: Either[DecodeStatus, String],
                               packetId: Option[ProtocolLevel],
                               payload: ByteString)
      extends DecodeStatus(isError = true)

  // 1.5.3 UTF-8 encoded strings
  implicit class MqttString(val v: String) extends AnyVal {

    def encode(bsb: ByteStringBuilder): ByteStringBuilder = {
      val length = v.length & 0xFFFF
      bsb.putShort(length).putBytes(v.getBytes(StandardCharsets.UTF_8), 0, length)
    }
  }

  // 2 MQTT Control Packet format
  implicit class MqttControlPacket(val v: ControlPacket) extends AnyVal {

    def encode(bsb: ByteStringBuilder, remainingLength: Int): ByteStringBuilder =
      bsb.putByte((v.packetType.underlying << 4 | v.flags.underlying).toByte).putByte(remainingLength.toByte)
  }

  // 3.1 CONNECT – Client requests a connection to a Server
  implicit class MqttConnect(val v: Connect) extends AnyVal {
    def encode(bsb: ByteStringBuilder): ByteStringBuilder = {
      val packetBsb = ByteString.newBuilder
      // Variable header
      Connect.Mqtt.encode(packetBsb)
      packetBsb.putByte(Connect.v311.toByte)
      packetBsb.putByte(v.connectFlags.underlying.toByte)
      packetBsb.putShort(v.keepAlive.toSeconds.toShort)
      // Payload
      v.clientId.encode(packetBsb)
      v.willTopic.foreach(_.encode(packetBsb))
      v.willMessage.foreach(_.encode(packetBsb))
      v.username.foreach(_.encode(packetBsb))
      v.password.foreach(_.encode(packetBsb))
      // Fixed header
      (v: ControlPacket).encode(bsb, packetBsb.length)
      bsb.append(packetBsb.result())
    }
  }

  // 3.2 CONNACK – Acknowledge connection request
  implicit class MqttConnAck(val v: ConnAck) extends AnyVal {
    def encode(bsb: ByteStringBuilder): ByteStringBuilder = {
      (v: ControlPacket).encode(bsb, 2)
      bsb.putByte(v.connectAckFlags.underlying.toByte)
      bsb.putByte(v.returnCode.underlying.toByte)
      bsb
    }
  }

  // 3.3 PUBLISH – Publish message
  implicit class MqttPublish(val v: Publish) extends AnyVal {
    def encode(bsb: ByteStringBuilder): ByteStringBuilder = {
      val packetBsb = ByteString.newBuilder
      // Variable header
      v.topicName.encode(packetBsb)
      v.packetId.foreach(pi => packetBsb.putShort(pi.underlying.toShort))
      // Payload
      packetBsb.append(v.payload)
      // Fixed header
      (v: ControlPacket).encode(bsb, packetBsb.length)
      bsb.append(packetBsb.result())
    }
  }

  // 3.4 PUBACK – Publish acknowledgement
  implicit class MqttPubAck(val v: PubAck) extends AnyVal {
    def encode(bsb: ByteStringBuilder): ByteStringBuilder = {
      (v: ControlPacket).encode(bsb, 2)
      bsb.putShort(v.packetId.underlying.toShort)
      bsb
    }
  }

  // 3.5 PUBREC – Publish received (QoS 2 publish received, part 1)
  implicit class MqttPubRec(val v: PubRec) extends AnyVal {
    def encode(bsb: ByteStringBuilder): ByteStringBuilder = {
      (v: ControlPacket).encode(bsb, 2)
      bsb.putShort(v.packetId.underlying.toShort)
      bsb
    }
  }

  // 3.6 PUBREL – Publish release (QoS 2 publish received, part 2)
  implicit class MqttPubRel(val v: PubRel) extends AnyVal {
    def encode(bsb: ByteStringBuilder): ByteStringBuilder = {
      (v: ControlPacket).encode(bsb, 2)
      bsb.putShort(v.packetId.underlying.toShort)
      bsb
    }
  }

  implicit class MqttByteIterator(val v: ByteIterator) extends AnyVal {

    // 1.5.3 UTF-8 encoded strings
    def decodeString(): Either[DecodeStatus, String] =
      try {
        val length = v.getShort & 0xffff
        Right(v.getByteString(length).utf8String)
      } catch {
        case _: NoSuchElementException => Left(BufferUnderflow)
      }

    // 2 MQTT Control Packet format
    def decodeControlPacket(): Either[DecodeStatus, ControlPacket] =
      try {
        val b = v.getByte & 0xff
        val l0 = v.getByte & 0xff
        val l1 = if ((l0 & 0x80) == 0x80) v.getByte & 0xff else 0
        val l2 = if ((l1 & 0x80) == 0x80) v.getByte & 0xff else 0
        val l3 = if ((l2 & 0x80) == 0x80) v.getByte & 0xff else 0
        val l = (l3 << 24) | (l2 << 16) | (l1 << 8) | l0
        (ControlPacketType(b >> 4), ControlPacketFlags(b & 0xf)) match {
          case (ControlPacketType.Reserved1, ControlPacketFlags.Reserved) =>
            Right(Reserved1)
          case (ControlPacketType.Reserved2, ControlPacketFlags.Reserved) =>
            Right(Reserved2)
          case (ControlPacketType.CONNECT, ControlPacketFlags.Reserved) =>
            v.decodeConnect()
          case (ControlPacketType.CONNACK, ControlPacketFlags.Reserved) =>
            v.decodeConnAck()
          case (ControlPacketType.PUBLISH, flags) =>
            v.decodePublish(l, flags)
          case (ControlPacketType.PUBACK, ControlPacketFlags.Reserved) =>
            v.decodePubAck()
          case (ControlPacketType.PUBREC, ControlPacketFlags.Reserved) =>
            v.decodePubRec()
          case (ControlPacketType.PUBREL, ControlPacketFlags.ReservedPubRel) =>
            v.decodePubRel()
          case (packetType, flags) =>
            Left(UnknownPacketType(packetType, flags))
        }
      } catch {
        case _: NoSuchElementException => Left(BufferUnderflow)
      }

    // 3.1 CONNECT – Client requests a connection to a Server
    def decodeConnect(): Either[DecodeStatus, Connect] =
      try {
        val protocolName = v.decodeString()
        val protocolLevel = v.getByte & 0xff
        (protocolName, protocolLevel) match {
          case (Right(Connect.Mqtt), Connect.v311) =>
            val connectFlags = ConnectFlags(v.getByte & 0xff)
            if ((connectFlags.underlying & ConnectFlags.Reserved.underlying) == 0) {
              val keepAlive = FiniteDuration(v.getShort & 0xffff, TimeUnit.SECONDS)
              val clientId = v.decodeString()
              val willTopic =
                if ((connectFlags.underlying & ConnectFlags.WillFlag.underlying) == ConnectFlags.WillFlag.underlying)
                  Some(v.decodeString())
                else None
              val willMessage =
                if ((connectFlags.underlying & ConnectFlags.WillFlag.underlying) == ConnectFlags.WillFlag.underlying)
                  Some(v.decodeString())
                else None
              val username =
                if ((connectFlags.underlying & ConnectFlags.UsernameFlag.underlying) == ConnectFlags.UsernameFlag.underlying)
                  Some(v.decodeString())
                else None
              val password =
                if ((connectFlags.underlying & ConnectFlags.PasswordFlag.underlying) == ConnectFlags.PasswordFlag.underlying)
                  Some(v.decodeString())
                else None
              (clientId,
               willTopic.fold[Either[DecodeStatus, Option[String]]](Right(None))(_.map(Some.apply)),
               willMessage.fold[Either[DecodeStatus, Option[String]]](Right(None))(_.map(Some.apply)),
               username.fold[Either[DecodeStatus, Option[String]]](Right(None))(_.map(Some.apply)),
               password.fold[Either[DecodeStatus, Option[String]]](Right(None))(_.map(Some.apply))) match {
                case (Right(ci), Right(wt), Right(wm), Right(un), Right(pw)) =>
                  Right(Connect(Connect.Mqtt, Connect.v311, ci, connectFlags, keepAlive, wt, wm, un, pw))
                case _ =>
                  Left(BadConnectMessage(clientId, willTopic, willMessage, username, password))
              }
            } else {
              Left(ConnectFlagReservedSet)
            }
          case (pn, pl) =>
            Left(UnknownConnectProtocol(pn, pl))
        }
      } catch {
        case _: NoSuchElementException => Left(BufferUnderflow)
      }

    // 3.2 CONNACK – Acknowledge connection request
    def decodeConnAck(): Either[DecodeStatus, ConnAck] =
      try {
        val connectAckFlags = v.getByte & 0xff
        if ((connectAckFlags & 0xfe) == 0) {
          val resultCode = v.getByte & 0xff
          Right(ConnAck(ConnAckFlags(connectAckFlags), ConnAckReturnCode(resultCode)))
        } else {
          Left(ConnectAckFlagReservedBitsSet)
        }
      } catch {
        case _: NoSuchElementException => Left(BufferUnderflow)
      }

    // 3.3 PUBLISH – Publish message
    def decodePublish(l: Int, flags: ControlPacketFlags): Either[DecodeStatus, Publish] =
      try {
        if ((flags.underlying & ControlPacketFlags.QoSReserved.underlying) != ControlPacketFlags.QoSReserved.underlying) {
          val packetLen = v.len
          val topicName = v.decodeString()
          val packetId =
            if ((flags.underlying & (ControlPacketFlags.QoSAtLeastOnceDelivery | ControlPacketFlags.QoSExactlyOnceDelivery).underlying) > 0)
              Some(v.getShort & 0xffff)
            else None
          val payload = v.getByteString(l - (packetLen - v.len))
          (topicName, packetId, payload) match {
            case (Right(tn), pi, p) =>
              Right(Publish(flags, tn, pi.map(PacketId.apply), p))
            case _ =>
              Left(BadPublishMessage(topicName, packetId, payload))
          }
        } else {
          Left(InvalidQoS)
        }
      } catch {
        case _: NoSuchElementException => Left(BufferUnderflow)
      }

    // 3.4 PUBACK – Publish acknowledgement
    def decodePubAck(): Either[DecodeStatus, PubAck] =
      try {
        val packetId = v.getShort & 0xffff
        Right(PubAck(PacketId(packetId)))
      } catch {
        case _: NoSuchElementException => Left(BufferUnderflow)
      }

    // 3.5 PUBREC – Publish received (QoS 2 publish received, part 1)
    def decodePubRec(): Either[DecodeStatus, PubRec] =
      try {
        val packetId = v.getShort & 0xffff
        Right(PubRec(PacketId(packetId)))
      } catch {
        case _: NoSuchElementException => Left(BufferUnderflow)
      }

    // 3.6 PUBREL – Publish release (QoS 2 publish received, part 2)
    def decodePubRel(): Either[DecodeStatus, PubRel] =
      try {
        val packetId = v.getShort & 0xffff
        Right(PubRel(PacketId(packetId)))
      } catch {
        case _: NoSuchElementException => Left(BufferUnderflow)
      }
  }
}
