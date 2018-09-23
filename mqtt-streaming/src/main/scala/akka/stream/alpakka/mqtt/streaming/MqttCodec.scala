/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.mqtt.streaming
import java.nio.ByteOrder
import java.nio.charset.StandardCharsets
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
  val Reserved = ControlPacketFlags(0x0)

}
final case class ControlPacketFlags(underlying: Int) extends AnyVal

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
   *  Cannot determine the type/flags combination of the control packet
   */
  final case class UnknownPacketType(packetType: ControlPacketType, flags: ControlPacketFlags)
      extends DecodeStatus(isError = true)

  /**
   *  Cannot determine the protocol name/level combination of the connect
   */
  final case class UnknownConnectProtocol(protocolName: Either[DecodeStatus, String], protocolLevel: ProtocolLevel)
      extends DecodeStatus(isError = true)

  /**
   * Bit 0 of the connect flag was set - which it should not be as it is reserved.
   */
  final case object ConnectFlagReservedSet extends DecodeStatus(isError = true)

  /**
   *  Something is wrong with the connect message
   */
  final case class BadConnectMessage(clientId: Either[MqttCodec.DecodeStatus, String],
                                     willTopic: Option[Either[MqttCodec.DecodeStatus, String]],
                                     willMessage: Option[Either[MqttCodec.DecodeStatus, String]],
                                     username: Option[Either[MqttCodec.DecodeStatus, String]],
                                     password: Option[Either[MqttCodec.DecodeStatus, String]])
      extends DecodeStatus(isError = true)

  /**
   * Bits 1  to 7 are set with the Connect Ack flags
   */
  case object ConnectAckFlagReservedBitsSet extends DecodeStatus(isError = true)

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

  implicit class MqttByteIterator(val v: ByteIterator) extends AnyVal {

    // 1.5.3 UTF-8 encoded strings
    def decodeString(): Either[DecodeStatus, String] =
      if (v.len >= 2) {
        val length = v.getShort & 0xffff
        if (v.len >= length) {
          Right(v.getByteString(length).utf8String)
        } else {
          Left(BufferUnderflow)
        }
      } else {
        Left(BufferUnderflow)
      }

    // 2 MQTT Control Packet format
    def decodeControlPacket(): Either[DecodeStatus, ControlPacket] =
      if (v.len >= 2) {
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
            v.decodeConnect(l)
          case (ControlPacketType.CONNACK, ControlPacketFlags.Reserved) =>
            v.decodeConnectAck(l)
          case (packetType, flags) =>
            Left(UnknownPacketType(packetType, flags))
        }
      } else {
        Left(BufferUnderflow)
      }

    // 3.1 CONNECT – Client requests a connection to a Server
    def decodeConnect(l: Int): Either[DecodeStatus, Connect] =
      if (v.len >= l) {
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
               willTopic.flatMap(x => x.toOption),
               willMessage.flatMap(x => x.toOption),
               username.flatMap(x => x.toOption),
               password.flatMap(x => x.toOption)) match {
                case (Right(ci), wt, wm, un, pw) =>
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
      } else {
        Left(BufferUnderflow)
      }

    // 3.2 CONNACK – Acknowledge connection request
    def decodeConnectAck(l: Int): Either[DecodeStatus, ConnAck] =
      if (v.len >= l) {
        val connectAckFlags = v.getByte & 0xff
        if ((connectAckFlags & 0xfe) == 0) {
          val resultCode = v.getByte & 0xff
          Right(ConnAck(ConnAckFlags(connectAckFlags), ConnAckReturnCode(resultCode)))
        } else {
          Left(ConnectAckFlagReservedBitsSet)
        }
      } else {
        Left(BufferUnderflow)
      }
  }
}
