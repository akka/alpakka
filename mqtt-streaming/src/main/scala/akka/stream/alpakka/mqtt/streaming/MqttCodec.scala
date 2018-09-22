/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.mqtt.streaming
import java.nio.ByteOrder
import java.nio.charset.StandardCharsets
import java.util.concurrent.TimeUnit

import akka.stream.alpakka.mqtt.streaming.Connect.ProtocolLevel
import akka.util.{ByteIterator, ByteStringBuilder}

import scala.concurrent.duration._

/**
 * 2.2.1 MQTT Control Packet type
 */
object ControlPacketType {
  private[streaming] val Reserved1 = ControlPacketType(0)
  private[streaming] val CONNECT = ControlPacketType(1)
  private[streaming] val CONNACK = ControlPacketType(2)
  private[streaming] val PUBLISH = ControlPacketType(3)
  private[streaming] val PUBACK = ControlPacketType(4)
  private[streaming] val PUBREC = ControlPacketType(5)
  private[streaming] val PUBREL = ControlPacketType(6)
  private[streaming] val PUBCOMP = ControlPacketType(7)
  private[streaming] val SUBSCRIBE = ControlPacketType(8)
  private[streaming] val SUBACK = ControlPacketType(9)
  private[streaming] val UNSUBSCRIBE = ControlPacketType(10)
  private[streaming] val UNSUBACK = ControlPacketType(11)
  private[streaming] val PINGREQ = ControlPacketType(12)
  private[streaming] val PINGRESP = ControlPacketType(13)
  private[streaming] val DISCONNECT = ControlPacketType(14)
  private[streaming] val Reserved2 = ControlPacketType(15)
}
final case class ControlPacketType(underlying: Int) extends AnyVal

/**
 * 2.2.2 Flags
 */
object ControlPacketFlags {
  private[streaming] val Reserved = ControlPacketFlags(0x0)

}
final case class ControlPacketFlags(underlying: Int) extends AnyVal

/**
 * 2 MQTT Control Packet format
 */
sealed abstract class ControlPacket(val packetType: ControlPacketType, val flags: ControlPacketFlags) {
  // 2.2 Fixed header
  def remainingLength: Int
}

case object Reserved1 extends ControlPacket(ControlPacketType.Reserved1, ControlPacketFlags.Reserved) {
  override def remainingLength: Int =
    0
}

case object Reserved2 extends ControlPacket(ControlPacketType.Reserved2, ControlPacketFlags.Reserved) {
  override def remainingLength: Int =
    0
}

object ConnectFlags {
  private[streaming] val None = ConnectFlags(0)
  private[streaming] val Reserved = ConnectFlags(0)
  private[streaming] val CleanSession = ConnectFlags(1 << 1)
  private[streaming] val WillFlag = ConnectFlags(1 << 2)
  private[streaming] val WillQoS = ConnectFlags(2 << 3)
  private[streaming] val WillRetain = ConnectFlags(1 << 5)
  private[streaming] val PasswordFlag = ConnectFlags(1 << 6)
  private[streaming] val UsernameFlag = ConnectFlags(1 << 7)
}

/**
 * 3.1.2.3 Connect Flags
 */
final case class ConnectFlags(underlying: Int) extends AnyVal

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
      ConnectFlags(
        extraConnectFlags.underlying | ConnectFlags.UsernameFlag.underlying | ConnectFlags.PasswordFlag.underlying
      ),
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
case class Connect(protocolName: Connect.ProtocolName,
                   protocolLevel: Connect.ProtocolLevel,
                   clientId: String,
                   connectFlags: ConnectFlags,
                   keepAlive: FiniteDuration,
                   willTopic: Option[String],
                   willMessage: Option[String],
                   username: Option[String],
                   password: Option[String])
    extends ControlPacket(ControlPacketType.CONNECT, ControlPacketFlags.Reserved) {

  import Connect._

  override def remainingLength: Int =
    (2 + Mqtt.length) +
    1 +
    1 +
    2 +
    (2 + clientId.length) +
    (2 + willTopic.fold(0)(_.length)) +
    (2 + willMessage.fold(0)(_.length)) +
    (2 + username.fold(0)(_.length)) +
    (2 + password.fold(0)(_.length))
}

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

  // 1.5.3 UTF-8 encoded strings
  implicit class MqttString(val v: String) extends AnyVal {

    def encode(bsb: ByteStringBuilder): ByteStringBuilder = {
      val length = v.length & 0xFFFF
      bsb.putShort(length).putBytes(v.getBytes(StandardCharsets.UTF_8), 0, length)
    }
  }

  // 2 MQTT Control Packet format
  implicit class MqttControlPacket(val v: ControlPacket) extends AnyVal {

    def encode(bsb: ByteStringBuilder): ByteStringBuilder =
      bsb.putByte((v.packetType.underlying << 4 | v.flags.underlying).toByte).putByte(v.remainingLength.toByte)
  }

  // 3.1 CONNECT – Client requests a connection to a Server
  implicit class MqttConnect(val v: Connect) extends AnyVal {
    def encode(bsb: ByteStringBuilder): ByteStringBuilder = {
      // Fixed header
      v.encode(bsb)
      // Variable header
      Connect.Mqtt.encode(bsb)
      bsb.putByte(Connect.v311.toByte)
      bsb.putByte(v.flags.underlying.toByte)
      bsb.putShort(v.keepAlive.toSeconds.toShort)
      // Payload
      v.clientId.encode(bsb)
      v.willTopic.foreach(_.encode(bsb))
      v.willMessage.foreach(_.encode(bsb))
      v.username.foreach(_.encode(bsb))
      v.password.foreach(_.encode(bsb))
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
            if ((connectFlags.underlying & 0x01) == 0) {
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
              if (clientId.isRight && willTopic.fold(true)(_.isRight) && willMessage.fold(true)(_.isRight) && username
                    .fold(true)(_.isRight) && password.fold(true)(_.isRight)) {
                Right(
                  Connect(
                    Connect.Mqtt,
                    Connect.v311,
                    clientId.getOrElse(""),
                    connectFlags,
                    keepAlive,
                    willTopic.map(_.getOrElse("")),
                    willMessage.map(_.getOrElse("")),
                    username.map(_.getOrElse("")),
                    password.map(_.getOrElse(""))
                  )
                )
              } else {
                Left(BadConnectMessage(clientId, willTopic, willMessage, username, password))
              }
            } else {
              Left(ConnectFlagReservedSet)
            }
          case (pn, pl) =>
            Left(UnknownConnectProtocol(pn, pl))
        }
        Left(BufferUnderflow)
      } else {
        Left(BufferUnderflow)
      }
  }
}
