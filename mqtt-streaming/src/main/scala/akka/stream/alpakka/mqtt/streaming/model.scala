/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.mqtt.streaming
import java.nio.ByteOrder
import java.nio.charset.StandardCharsets
import java.util.{NoSuchElementException, Optional}
import java.util.concurrent.{CompletionStage, ForkJoinPool, TimeUnit}

import akka.Done
import akka.annotation.InternalApi
import akka.japi.{Pair => AkkaPair}
import akka.stream.alpakka.mqtt.streaming.Connect.ProtocolLevel
import akka.util.{ByteIterator, ByteString, ByteStringBuilder}

import scala.annotation.tailrec
import scala.concurrent.duration._
import scala.collection.immutable
import scala.collection.JavaConverters._
import scala.compat.java8.OptionConverters._
import scala.concurrent.{ExecutionContext, Promise}

/**
 * 2.2.1 MQTT Control Packet type
 * http://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html
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
final case class ControlPacketType private (underlying: Int) extends AnyVal

/**
 * 2.2.2 Flags
 * http://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html
 */
object ControlPacketFlags {
  val None = ControlPacketFlags(0)
  val ReservedGeneral = ControlPacketFlags(0)
  val ReservedPubRel = ControlPacketFlags(1 << 1)
  val ReservedSubscribe = ControlPacketFlags(1 << 1)
  val ReservedUnsubscribe = ControlPacketFlags(1 << 1)
  val ReservedUnsubAck = ControlPacketFlags(1 << 1)

  /** "If the DUP flag is set to 1, it indicates that this might be re-delivery of an earlier attempt to send the Packet." */
  val DUP = ControlPacketFlags(1 << 3)
  val QoSAtMostOnceDelivery = ControlPacketFlags(0)
  val QoSAtLeastOnceDelivery = ControlPacketFlags(1 << 1)
  val QoSExactlyOnceDelivery = ControlPacketFlags(2 << 1)
  val QoSReserved = ControlPacketFlags(3 << 1)
  val QoSFailure = ControlPacketFlags(1 << 7)

  /** "If the RETAIN flag is set to 1, in a PUBLISH Packet sent by a Client to a Server, the Server MUST store the Application Message and its QoS." */
  val RETAIN = ControlPacketFlags(1)
}

final case class ControlPacketFlags private (underlying: Int) extends AnyVal {

  /**
   * Convenience bitwise OR
   */
  def |(rhs: ControlPacketFlags): ControlPacketFlags =
    ControlPacketFlags(underlying | rhs.underlying)

  /**
   * Convenience bitwise AND
   */
  def &(rhs: ControlPacketFlags): ControlPacketFlags =
    ControlPacketFlags(underlying & rhs.underlying)

  /**
   * Convenience for testing bits - returns true if all passed in are set
   */
  def contains(bits: ControlPacketFlags): Boolean =
    (underlying & bits.underlying) == bits.underlying
}

/**
 * 2 MQTT Control Packet format
 * http://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html
 */
sealed abstract class ControlPacket(val packetType: ControlPacketType, val flags: ControlPacketFlags)

case object Reserved1 extends ControlPacket(ControlPacketType.Reserved1, ControlPacketFlags.ReservedGeneral)

case object Reserved2 extends ControlPacket(ControlPacketType.Reserved2, ControlPacketFlags.ReservedGeneral)

/**
 * 2.3.1 Packet Identifier
 * http://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html
 */
final case class PacketId private (underlying: Int) extends AnyVal

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
 * http://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html
 */
final case class ConnectFlags private (underlying: Int) extends AnyVal {

  /**
   * Convenience bitwise OR
   */
  def |(rhs: ConnectFlags): ConnectFlags =
    ConnectFlags(underlying | rhs.underlying)

  /**
   * Convenience bitwise AND
   */
  def &(rhs: ConnectFlags): ConnectFlags =
    ConnectFlags(underlying & rhs.underlying)

  /**
   * Convenience for testing bits - returns true if all passed in are set
   */
  def contains(bits: ConnectFlags): Boolean =
    (underlying & bits.underlying) == bits.underlying
}

object Connect {
  type ProtocolName = String
  val Mqtt: ProtocolName = "MQTT"

  type ProtocolLevel = Int
  val v311: ProtocolLevel = 4

  val DefaultConnectTimeout: FiniteDuration =
    60.seconds

  /**
   * Conveniently create a connect object without credentials.
   */
  def apply(clientId: String, connectFlags: ConnectFlags): Connect =
    new Connect(clientId, connectFlags)

  /**
   * Conveniently create a connect object with credentials. This function will also set the
   * corresponding username and password flags.
   */
  def apply(clientId: String, extraConnectFlags: ConnectFlags, username: String, password: String): Connect =
    new Connect(clientId, extraConnectFlags, username, password)
}

/**
 * 3.1 CONNECT – Client requests a connection to a Server
 * http://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html
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
    extends ControlPacket(ControlPacketType.CONNECT, ControlPacketFlags.ReservedGeneral) {

  /**
   * Conveniently create a connect object without credentials.
   */
  def this(clientId: String, connectFlags: ConnectFlags) =
    this(Connect.Mqtt, Connect.v311, clientId, connectFlags, Connect.DefaultConnectTimeout, None, None, None, None)

  /**
   * Conveniently create a connect object with credentials. This function will also set the
   * corresponding username and password flags.
   */
  def this(clientId: String, extraConnectFlags: ConnectFlags, username: String, password: String) =
    this(
      Connect.Mqtt,
      Connect.v311,
      clientId,
      extraConnectFlags | ConnectFlags.UsernameFlag | ConnectFlags.PasswordFlag,
      Connect.DefaultConnectTimeout,
      None,
      None,
      Some(username),
      Some(password)
    )

  override def toString: String =
    s"""Connect(protocolName:$protocolName,protocolLevel:$protocolLevel,clientId:$clientId,connectFlags:$connectFlags,keepAlive:$keepAlive,willTopic:$willTopic,willMessage:$willMessage,username:$username,password:${password
      .map(_ => "********")})"""
}

object ConnAckFlags {
  val None = ConnAckFlags(0)
  val SessionPresent = ConnAckFlags(1)
}

/**
 * 3.2.2.1 Connect Acknowledge Flags
 * http://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html
 */
final case class ConnAckFlags private (underlying: Int) extends AnyVal

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
 * http://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html
 */
final case class ConnAckReturnCode private (underlying: Int) extends AnyVal {

  /**
   * Convenience bitwise OR
   */
  def |(rhs: ConnAckReturnCode): ConnAckReturnCode =
    ConnAckReturnCode(underlying | rhs.underlying)

  /**
   * Convenience bitwise AND
   */
  def &(rhs: ConnAckReturnCode): ConnAckReturnCode =
    ConnAckReturnCode(underlying & rhs.underlying)

  /**
   * Convenience for testing bits - returns true if all passed in are set
   */
  def contains(bits: ConnAckReturnCode): Boolean =
    (underlying & bits.underlying) == bits.underlying
}

/**
 * 3.2 CONNACK – Acknowledge connection request
 * http://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html
 */
final case class ConnAck(connectAckFlags: ConnAckFlags, returnCode: ConnAckReturnCode)
    extends ControlPacket(ControlPacketType.CONNACK, ControlPacketFlags.ReservedGeneral)

object Publish {

  /**
   * 3.3 PUBLISH – Publish message
   * http://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html
   */
  def apply(flags: ControlPacketFlags, topicName: String, payload: ByteString): Publish =
    new Publish(flags, topicName, payload)

  /**
   * Conveniently create a publish message with at least once delivery
   */
  def apply(topicName: String, payload: ByteString): Publish =
    new Publish(topicName, payload)
}

/**
 * 3.3 PUBLISH – Publish message
 * http://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html
 */
final case class Publish @InternalApi private[streaming] (override val flags: ControlPacketFlags,
                                                          topicName: String,
                                                          packetId: Option[PacketId],
                                                          payload: ByteString)
    extends ControlPacket(ControlPacketType.PUBLISH, flags) {

  /**
   * 3.3 PUBLISH – Publish message
   * http://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html
   */
  def this(flags: ControlPacketFlags, topicName: String, payload: ByteString) =
    this(flags, topicName, None, payload)

  /**
   * Conveniently create a publish message with at least once delivery
   */
  def this(topicName: String, payload: ByteString) =
    this(ControlPacketFlags.QoSAtLeastOnceDelivery, topicName, Some(PacketId(0)), payload)

  override def toString: String =
    s"""Publish(flags:$flags,topicName:$topicName,packetId:$packetId,payload:${payload.size}b)"""
}

/**
 * 3.4 PUBACK – Publish acknowledgement
 * http://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html
 */
final case class PubAck(packetId: PacketId)
    extends ControlPacket(ControlPacketType.PUBACK, ControlPacketFlags.ReservedGeneral)

/**
 * 3.5 PUBREC – Publish received (QoS 2 publish received, part 1)
 * http://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html
 */
final case class PubRec(packetId: PacketId)
    extends ControlPacket(ControlPacketType.PUBREC, ControlPacketFlags.ReservedGeneral)

/**
 * 3.6 PUBREL – Publish release (QoS 2 publish received, part 2)
 * http://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html
 */
final case class PubRel(packetId: PacketId)
    extends ControlPacket(ControlPacketType.PUBREL, ControlPacketFlags.ReservedPubRel)

/**
 * 3.7 PUBCOMP – Publish complete (QoS 2 publish received, part 3)
 * http://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html
 */
final case class PubComp(packetId: PacketId)
    extends ControlPacket(ControlPacketType.PUBCOMP, ControlPacketFlags.ReservedGeneral)

object Subscribe {

  /**
   * 3.8 SUBSCRIBE - Subscribe to topics
   * http://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html
   */
  def apply(topicFilters: immutable.Seq[(String, ControlPacketFlags)]): Subscribe =
    new Subscribe(topicFilters)

  /**
   *  A convenience for subscribing to a single topic with at-least-once semantics
   */
  def apply(topicFilter: String): Subscribe =
    new Subscribe(topicFilter)
}

/**
 * 3.8 SUBSCRIBE - Subscribe to topics
 * http://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html
 */
final case class Subscribe @InternalApi private[streaming] (packetId: PacketId,
                                                            topicFilters: immutable.Seq[(String, ControlPacketFlags)])
    extends ControlPacket(ControlPacketType.SUBSCRIBE, ControlPacketFlags.ReservedSubscribe) {

  /**
   * 3.8 SUBSCRIBE - Subscribe to topics
   * http://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html
   */
  def this(topicFilters: immutable.Seq[(String, ControlPacketFlags)]) =
    this(PacketId(0), topicFilters)

  /**
   * JAVA API
   *
   * 3.8 SUBSCRIBE - Subscribe to topics
   * http://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html
   */
  def this(topicFilters: java.util.List[AkkaPair[String, Integer]]) =
    this(PacketId(0), topicFilters.asScala.toIndexedSeq.map(v => v.first -> ControlPacketFlags(v.second)))

  /**
   * A convenience for subscribing to a single topic with at-least-once semantics
   */
  def this(topicFilter: String) =
    this(PacketId(0), List(topicFilter -> ControlPacketFlags.QoSAtLeastOnceDelivery))
}

/**
 * 3.9 SUBACK – Subscribe acknowledgement
 * http://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html
 */
final case class SubAck(packetId: PacketId, returnCodes: immutable.Seq[ControlPacketFlags])
    extends ControlPacket(ControlPacketType.SUBACK, ControlPacketFlags.ReservedGeneral) {

  /**
   * JAVA API
   *
   * 3.9 SUBACK – Subscribe acknowledgement
   * http://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html
   */
  def this(packetId: PacketId, returnCodes: java.util.List[Integer]) =
    this(packetId, returnCodes.asScala.toIndexedSeq.map(v => ControlPacketFlags(v)))
}

object Unsubscribe {

  /**
   * 3.10 UNSUBSCRIBE – Unsubscribe from topics
   * http://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html
   */
  def apply(topicFilters: immutable.Seq[String]): Unsubscribe =
    new Unsubscribe(topicFilters)

  /**
   * A convenience for unsubscribing from a single topic
   */
  def apply(topicFilter: String): Unsubscribe =
    new Unsubscribe(topicFilter)
}

/**
 * 3.10 UNSUBSCRIBE – Unsubscribe from topics
 * http://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html
 */
final case class Unsubscribe @InternalApi private[streaming] (packetId: PacketId, topicFilters: immutable.Seq[String])
    extends ControlPacket(ControlPacketType.UNSUBSCRIBE, ControlPacketFlags.ReservedUnsubscribe) {

  /**
   * 3.10 UNSUBSCRIBE – Unsubscribe from topics
   * http://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html
   */
  def this(topicFilters: immutable.Seq[String]) =
    this(PacketId(0), topicFilters)

  /**
   * JAVA API
   *
   * 3.10 UNSUBSCRIBE – Unsubscribe from topics
   * http://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html
   */
  def this(topicFilters: java.util.List[String]) =
    this(PacketId(0), topicFilters.asScala.toIndexedSeq)

  /**
   * A convenience for unsubscribing from a single topic
   */
  def this(topicFilter: String) =
    this(PacketId(0), List(topicFilter))
}

/**
 * 3.11 UNSUBACK – Unsubscribe acknowledgement
 * http://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html
 */
final case class UnsubAck(packetId: PacketId)
    extends ControlPacket(ControlPacketType.UNSUBACK, ControlPacketFlags.ReservedUnsubAck)

/**
 * 3.12 PINGREQ – PING request
 * http://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html
 */
case object PingReq extends ControlPacket(ControlPacketType.PINGREQ, ControlPacketFlags.ReservedGeneral)

/**
 * 3.13 PINGRESP – PING response
 * http://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html
 */
case object PingResp extends ControlPacket(ControlPacketType.PINGRESP, ControlPacketFlags.ReservedGeneral)

/**
 * 3.14 DISCONNECT – Disconnect notification
 * http://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html
 */
case object Disconnect extends ControlPacket(ControlPacketType.DISCONNECT, ControlPacketFlags.ReservedGeneral)

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
  sealed abstract class DecodeError

  /**
   * Not enough bytes in the byte iterator
   */
  final case object BufferUnderflow extends DecodeError

  /**
   * Cannot determine the type/flags combination of the control packet
   */
  final case class UnknownPacketType(packetType: ControlPacketType, flags: ControlPacketFlags) extends DecodeError

  /**
   * A message has been received that exceeds the maximum we have chosen--which is
   * typically much less than what the spec permits. The reported sizes do not
   * include the fixed header size of 2 bytes.
   */
  final case class InvalidPacketSize(packetSize: Int, maxPacketSize: Int) extends DecodeError

  /**
   * Cannot determine the protocol name/level combination of the connect
   */
  final case class UnknownConnectProtocol(protocolName: Either[DecodeError, String], protocolLevel: ProtocolLevel)
      extends DecodeError

  /**
   * Bit 0 of the connect flag was set - which it should not be as it is reserved.
   */
  final case object ConnectFlagReservedSet extends DecodeError

  /**
   * Something is wrong with the connect message
   */
  final case class BadConnectMessage(clientId: Either[MqttCodec.DecodeError, String],
                                     willTopic: Option[Either[MqttCodec.DecodeError, String]],
                                     willMessage: Option[Either[MqttCodec.DecodeError, String]],
                                     username: Option[Either[MqttCodec.DecodeError, String]],
                                     password: Option[Either[MqttCodec.DecodeError, String]])
      extends DecodeError {
    override def toString: String =
      s"""BadConnectMessage(clientId:$clientId,willTopic:$willTopic,willMessage:$willMessage,username:$username,password:${password
        .map {
          case Left(x) => s"Left($x)"
          case Right(x) => s"Right(" + x.map(_ => "********") + ")"
        }})"""
  }

  /**
   * A reserved QoS was specified
   */
  case object InvalidQoS extends DecodeError

  /**
   * Bits 1 to 7 are set with the Connect Ack flags
   */
  case object ConnectAckFlagReservedBitsSet extends DecodeError

  /**
   * Something is wrong with the publish message
   */
  final case class BadPublishMessage(topicName: Either[DecodeError, String],
                                     packetId: Option[PacketId],
                                     payload: ByteString)
      extends DecodeError {
    override def toString: String =
      s"""BadPublishMessage(topicName:$topicName,packetId:$packetId,payload:${payload.size}b)"""
  }

  /**
   * Something is wrong with the subscribe message
   */
  final case class BadSubscribeMessage(packetId: PacketId,
                                       topicFilters: immutable.Seq[(Either[DecodeError, String], ControlPacketFlags)])
      extends DecodeError

  /**
   * Unable to subscribe at the requested QoS
   * @deprecated this message was never able to be returned - always use [[SubAck]] to test subscribed QoS, since 1.1.1
   */
  @deprecated("this message was never able to be returned - always use [[SubAck]] to test subscribed QoS", "1.1.1")
  final case class BadSubAckMessage(packetId: PacketId, returnCodes: immutable.Seq[ControlPacketFlags])
      extends DecodeError

  /**
   * Something is wrong with the unsubscribe message
   */
  final case class BadUnsubscribeMessage(packetId: PacketId, topicFilters: immutable.Seq[Either[DecodeError, String]])
      extends DecodeError

  /**
   * JAVA API
   */
  final case class DecodeErrorOrControlPacket(v: Either[DecodeError, ControlPacket]) {
    def getDecodeError: Optional[DecodeError] =
      v match {
        case Right(_) => Optional.empty()
        case Left(de) => Optional.of(de)
      }

    def getControlPacket: Optional[ControlPacket] =
      v match {
        case Right(cp) => Optional.of(cp)
        case Left(_) => Optional.empty()
      }
  }

  // 1.5.3 UTF-8 encoded strings
  implicit class MqttString(val v: String) extends AnyVal {

    def encode(bsb: ByteStringBuilder): ByteStringBuilder = {
      val length = v.length & 0xffff
      bsb.putShort(length).putBytes(v.getBytes(StandardCharsets.UTF_8), 0, length)
    }
  }

  // 2 MQTT Control Packet format
  implicit class MqttControlPacket(val v: ControlPacket) extends AnyVal {

    def encode(bsb: ByteStringBuilder, remainingLength: Int): ByteStringBuilder = {
      bsb.putByte((v.packetType.underlying << 4 | v.flags.underlying).toByte)
      remainingLength.encode(bsb)
    }
  }

  // 2.2.3 Remaining Length
  implicit class MqttRemainingLength(val v: Int) extends AnyVal {

    def encode(bsb: ByteStringBuilder): ByteStringBuilder = {
      val b0 = v
      val b1 = v >> 7
      val b2 = v >> 14
      val b3 = v >> 21
      bsb.putByte(((b0 & 0x7f) | (if (b1 > 0) 0x80 else 0x00)).toByte)
      if (b1 > 0) bsb.putByte(((b1 & 0x7f) | (if (b2 > 0) 0x80 else 0x00)).toByte)
      if (b2 > 0) bsb.putByte(((b2 & 0x7f) | (if (b3 > 0) 0x80 else 0x00)).toByte)
      if (b3 > 0) bsb.putByte(b3.toByte)
      bsb
    }
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
    def encode(bsb: ByteStringBuilder, packetId: Option[PacketId]): ByteStringBuilder = {
      val packetBsb = ByteString.newBuilder
      // Variable header
      v.topicName.encode(packetBsb)
      packetId.foreach(pi => packetBsb.putShort(pi.underlying.toShort))
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

  // 3.7 PUBCOMP – Publish complete (QoS 2 publish received, part 3)
  implicit class MqttPubComp(val v: PubComp) extends AnyVal {
    def encode(bsb: ByteStringBuilder): ByteStringBuilder = {
      (v: ControlPacket).encode(bsb, 2)
      bsb.putShort(v.packetId.underlying.toShort)
      bsb
    }
  }

  // 3.8 SUBSCRIBE - Subscribe to topics
  implicit class MqttSubscribe(val v: Subscribe) extends AnyVal {
    def encode(bsb: ByteStringBuilder, packetId: PacketId): ByteStringBuilder = {
      val packetBsb = ByteString.newBuilder
      // Variable header
      packetBsb.putShort(packetId.underlying.toShort)
      // Payload
      v.topicFilters.foreach {
        case (topicFilter, topicFilterFlags) =>
          topicFilter.encode(packetBsb)
          packetBsb.putByte(topicFilterFlags.underlying.toByte)
      }
      // Fixed header
      (v: ControlPacket).encode(bsb, packetBsb.length)
      bsb.append(packetBsb.result())
    }
  }

  // 3.9 SUBACK – Subscribe acknowledgement
  implicit class MqttSubAck(val v: SubAck) extends AnyVal {
    def encode(bsb: ByteStringBuilder): ByteStringBuilder = {
      val packetBsb = ByteString.newBuilder
      // Variable header
      packetBsb.putShort(v.packetId.underlying.toShort)
      // Payload
      v.returnCodes.foreach { returnCode =>
        packetBsb.putByte(returnCode.underlying.toByte)
      }
      // Fixed header
      (v: ControlPacket).encode(bsb, packetBsb.length)
      bsb.append(packetBsb.result())
    }
  }

  // 3.10 UNSUBSCRIBE – Unsubscribe from topics
  implicit class MqttUnsubscribe(val v: Unsubscribe) extends AnyVal {
    def encode(bsb: ByteStringBuilder, packetId: PacketId): ByteStringBuilder = {
      val packetBsb = ByteString.newBuilder
      // Variable header
      packetBsb.putShort(packetId.underlying.toShort)
      // Payload
      v.topicFilters.foreach(_.encode(packetBsb))
      // Fixed header
      (v: ControlPacket).encode(bsb, packetBsb.length)
      bsb.append(packetBsb.result())
    }
  }

  // 3.11 UNSUBACK – Unsubscribe acknowledgement
  implicit class MqttUnsubAck(val v: UnsubAck) extends AnyVal {
    def encode(bsb: ByteStringBuilder): ByteStringBuilder = {
      (v: ControlPacket).encode(bsb, 2)
      bsb.putShort(v.packetId.underlying.toShort)
      bsb
    }
  }

  // 3.12 PINGREQ – PING request
  implicit class MqttPingReq(val v: PingReq.type) extends AnyVal {
    def encode(bsb: ByteStringBuilder): ByteStringBuilder =
      (v: ControlPacket).encode(bsb, 0)
  }

  // 3.13 PINGRESP – PING response
  implicit class MqttPingResp(val v: PingResp.type) extends AnyVal {
    def encode(bsb: ByteStringBuilder): ByteStringBuilder =
      (v: ControlPacket).encode(bsb, 0)
  }

  // 3.14 DISCONNECT – Disconnect notification
  implicit class MqttDisconnect(val v: Disconnect.type) extends AnyVal {
    def encode(bsb: ByteStringBuilder): ByteStringBuilder =
      (v: ControlPacket).encode(bsb, 0)
  }

  implicit class MqttByteIterator(val v: ByteIterator) extends AnyVal {

    // 1.5.3 UTF-8 encoded strings
    def decodeString(): Either[DecodeError, String] =
      try {
        val length = v.getShort & 0xffff
        Right(v.getByteString(length).utf8String)
      } catch {
        case _: NoSuchElementException => Left(BufferUnderflow)
      }

    // 2 MQTT Control Packet format
    def decodeControlPacket(maxPacketSize: Int): Either[DecodeError, ControlPacket] =
      try {
        val b = v.getByte & 0xff
        v.decodeRemainingLength() match {
          case Right(l) if l <= maxPacketSize =>
            (ControlPacketType(b >> 4), ControlPacketFlags(b & 0xf)) match {
              case (ControlPacketType.Reserved1, ControlPacketFlags.ReservedGeneral) =>
                Right(Reserved1)
              case (ControlPacketType.Reserved2, ControlPacketFlags.ReservedGeneral) =>
                Right(Reserved2)
              case (ControlPacketType.CONNECT, ControlPacketFlags.ReservedGeneral) =>
                v.decodeConnect()
              case (ControlPacketType.CONNACK, ControlPacketFlags.ReservedGeneral) =>
                v.decodeConnAck()
              case (ControlPacketType.PUBLISH, flags) =>
                v.decodePublish(l, flags)
              case (ControlPacketType.PUBACK, ControlPacketFlags.ReservedGeneral) =>
                v.decodePubAck()
              case (ControlPacketType.PUBREC, ControlPacketFlags.ReservedGeneral) =>
                v.decodePubRec()
              case (ControlPacketType.PUBREL, ControlPacketFlags.ReservedPubRel) =>
                v.decodePubRel()
              case (ControlPacketType.PUBCOMP, ControlPacketFlags.ReservedGeneral) =>
                v.decodePubComp()
              case (ControlPacketType.SUBSCRIBE, ControlPacketFlags.ReservedSubscribe) =>
                v.decodeSubscribe(l)
              case (ControlPacketType.SUBACK, ControlPacketFlags.ReservedGeneral) =>
                v.decodeSubAck(l)
              case (ControlPacketType.UNSUBSCRIBE, ControlPacketFlags.ReservedUnsubscribe) =>
                v.decodeUnsubscribe(l)
              case (ControlPacketType.UNSUBACK, ControlPacketFlags.ReservedUnsubAck) =>
                v.decodeUnsubAck()
              case (ControlPacketType.PINGREQ, ControlPacketFlags.ReservedGeneral) =>
                Right(PingReq)
              case (ControlPacketType.PINGRESP, ControlPacketFlags.ReservedGeneral) =>
                Right(PingResp)
              case (ControlPacketType.DISCONNECT, ControlPacketFlags.ReservedGeneral) =>
                Right(Disconnect)
              case (packetType, flags) =>
                Left(UnknownPacketType(packetType, flags))
            }
          case Right(l) =>
            Left(InvalidPacketSize(l, maxPacketSize))
          case Left(BufferUnderflow) => Left(BufferUnderflow)
        }
      } catch {
        case _: NoSuchElementException => Left(BufferUnderflow)
      }

    // 2.2.3 Remaining Length
    def decodeRemainingLength(): Either[DecodeError, Int] =
      try {
        val l0 = v.getByte & 0xff
        val l1 = if ((l0 & 0x80) == 0x80) v.getByte & 0xff else 0
        val l2 = if ((l1 & 0x80) == 0x80) v.getByte & 0xff else 0
        val l3 = if ((l2 & 0x80) == 0x80) v.getByte & 0xff else 0
        val l = (l3 << 21) | ((l2 & 0x7f) << 14) | ((l1 & 0x7f) << 7) | (l0 & 0x7f)
        Right(l)
      } catch {
        case _: NoSuchElementException => Left(BufferUnderflow)
      }

    // 3.1 CONNECT – Client requests a connection to a Server
    def decodeConnect(): Either[DecodeError, Connect] =
      try {
        val protocolName = v.decodeString()
        val protocolLevel = v.getByte & 0xff
        (protocolName, protocolLevel) match {
          case (Right(Connect.Mqtt), Connect.v311) =>
            val connectFlags = ConnectFlags(v.getByte & 0xff)
            if (!connectFlags.contains(ConnectFlags.Reserved)) {
              val keepAlive = FiniteDuration(v.getShort & 0xffff, TimeUnit.SECONDS)
              val clientId = v.decodeString()
              val willTopic =
                if (connectFlags.contains(ConnectFlags.WillFlag)) Some(v.decodeString()) else None
              val willMessage =
                if (connectFlags.contains(ConnectFlags.WillFlag)) Some(v.decodeString()) else None
              val username =
                if (connectFlags.contains(ConnectFlags.UsernameFlag)) Some(v.decodeString()) else None
              val password =
                if (connectFlags.contains(ConnectFlags.PasswordFlag)) Some(v.decodeString()) else None
              (clientId,
               willTopic.fold[Either[DecodeError, Option[String]]](Right(None))(_.right.map(Some.apply)),
               willMessage.fold[Either[DecodeError, Option[String]]](Right(None))(_.right.map(Some.apply)),
               username.fold[Either[DecodeError, Option[String]]](Right(None))(_.right.map(Some.apply)),
               password.fold[Either[DecodeError, Option[String]]](Right(None))(_.right.map(Some.apply))) match {
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
    def decodeConnAck(): Either[DecodeError, ConnAck] =
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
    def decodePublish(l: Int, flags: ControlPacketFlags): Either[DecodeError, Publish] =
      try {
        if (!flags.contains(ControlPacketFlags.QoSReserved)) {
          val packetLen = v.len
          val topicName = v.decodeString()
          val packetId =
            if (flags.contains(ControlPacketFlags.QoSAtLeastOnceDelivery) ||
                flags.contains(ControlPacketFlags.QoSExactlyOnceDelivery))
              Some(PacketId(v.getShort & 0xffff))
            else None
          val payload = v.getByteString(l - (packetLen - v.len))
          (topicName, packetId, payload) match {
            case (Right(tn), pi, p) =>
              Right(Publish(flags, tn, pi, p))
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
    def decodePubAck(): Either[DecodeError, PubAck] =
      try {
        val packetId = PacketId(v.getShort & 0xffff)
        Right(PubAck(packetId))
      } catch {
        case _: NoSuchElementException => Left(BufferUnderflow)
      }

    // 3.5 PUBREC – Publish received (QoS 2 publish received, part 1)
    def decodePubRec(): Either[DecodeError, PubRec] =
      try {
        val packetId = PacketId(v.getShort & 0xffff)
        Right(PubRec(packetId))
      } catch {
        case _: NoSuchElementException => Left(BufferUnderflow)
      }

    // 3.6 PUBREL – Publish release (QoS 2 publish received, part 2)
    def decodePubRel(): Either[DecodeError, PubRel] =
      try {
        val packetId = PacketId(v.getShort & 0xffff)
        Right(PubRel(packetId))
      } catch {
        case _: NoSuchElementException => Left(BufferUnderflow)
      }

    // 3.7 PUBCOMP – Publish complete (QoS 2 publish received, part 3)
    def decodePubComp(): Either[DecodeError, PubComp] =
      try {
        val packetId = PacketId(v.getShort & 0xffff)
        Right(PubComp(packetId))
      } catch {
        case _: NoSuchElementException => Left(BufferUnderflow)
      }

    // 3.8 SUBSCRIBE - Subscribe to topics
    def decodeSubscribe(l: Int): Either[DecodeError, Subscribe] =
      try {
        val packetLen = v.len
        val packetId = PacketId(v.getShort & 0xffff)
        @tailrec
        def decodeTopicFilters(
            remainingLen: Int,
            topicFilters: Vector[(Either[DecodeError, String], ControlPacketFlags)]
        ): Vector[(Either[DecodeError, String], ControlPacketFlags)] =
          if (remainingLen > 0) {
            val packetLenAtTopicFilter = v.len
            val topicFilter = (v.decodeString(), ControlPacketFlags(v.getByte & 0xff))
            decodeTopicFilters(remainingLen - (packetLenAtTopicFilter - v.len), topicFilters :+ topicFilter)
          } else {
            topicFilters
          }
        val topicFilters = decodeTopicFilters(l - (packetLen - v.len), Vector.empty)
        val topicFiltersValid = topicFilters.nonEmpty && topicFilters.foldLeft(true) {
            case (true, (Right(_), tff)) if tff.underlying < ControlPacketFlags.QoSReserved.underlying => true
            case _ => false
          }
        if (topicFiltersValid) {
          Right(Subscribe(packetId, topicFilters.flatMap {
            case (Right(tfs), tff) => List(tfs -> tff)
            case _ => List.empty
          }))
        } else {
          Left(BadSubscribeMessage(packetId, topicFilters))
        }
      } catch {
        case _: NoSuchElementException => Left(BufferUnderflow)
      }

    // 3.9 SUBACK – Subscribe acknowledgement
    def decodeSubAck(l: Int): Either[DecodeError, SubAck] =
      try {
        val packetLen = v.len
        val packetId = PacketId(v.getShort & 0xffff)
        @tailrec
        def decodeReturnCodes(remainingLen: Int, returnCodes: Vector[ControlPacketFlags]): Vector[ControlPacketFlags] =
          if (remainingLen > 0) {
            val packetLenAtTopicFilter = v.len
            val returnCode = ControlPacketFlags(v.getByte & 0xff)
            decodeReturnCodes(remainingLen - (packetLenAtTopicFilter - v.len), returnCodes :+ returnCode)
          } else {
            returnCodes
          }
        val returnCodes = decodeReturnCodes(l - (packetLen - v.len), Vector.empty)
        Right(SubAck(packetId, returnCodes))
      } catch {
        case _: NoSuchElementException => Left(BufferUnderflow)
      }

    // 3.10 UNSUBSCRIBE – Unsubscribe from topics
    def decodeUnsubscribe(l: Int): Either[DecodeError, Unsubscribe] =
      try {
        val packetLen = v.len
        val packetId = PacketId(v.getShort & 0xffff)
        @tailrec
        def decodeTopicFilters(
            remainingLen: Int,
            topicFilters: Vector[Either[DecodeError, String]]
        ): Vector[Either[DecodeError, String]] =
          if (remainingLen > 0) {
            val packetLenAtTopicFilter = v.len
            val topicFilter = v.decodeString()
            decodeTopicFilters(remainingLen - (packetLenAtTopicFilter - v.len), topicFilters :+ topicFilter)
          } else {
            topicFilters
          }
        val topicFilters = decodeTopicFilters(l - (packetLen - v.len), Vector.empty)
        val topicFiltersValid = topicFilters.nonEmpty && topicFilters.foldLeft(true) {
            case (true, Right(_)) => true
            case _ => false
          }
        if (topicFiltersValid) {
          Right(Unsubscribe(packetId, topicFilters.flatMap {
            case Right(tfs) => List(tfs)
            case _ => List.empty
          }))
        } else {
          Left(BadUnsubscribeMessage(packetId, topicFilters))
        }
      } catch {
        case _: NoSuchElementException => Left(BufferUnderflow)
      }

    // 3.11 UNSUBACK – Unsubscribe acknowledgement
    def decodeUnsubAck(): Either[DecodeError, UnsubAck] =
      try {
        val packetId = PacketId(v.getShort & 0xffff)
        Right(UnsubAck(packetId))
      } catch {
        case _: NoSuchElementException => Left(BufferUnderflow)
      }
  }
}

object Command {

  /**
   * Send a command to an MQTT session
   * @param command The command to send
   * @tparam A The type of data being carried through in general, but not here
   */
  def apply[A](command: ControlPacket): Command[A] =
    new Command(command)

  /**
   * Send a command to an MQTT session with data to carry through into
   * any related event.
   * @param command The command to send
   * @param carry The data to carry through
   * @tparam A The type of data to carry through
   */
  def apply[A](command: ControlPacket, carry: A): Command[A] =
    new Command(command, carry)
}

/**
 * Send a command to an MQTT session with optional data to carry through
 * into any related event.
 * @param command The command to send
 * @param completed A promise that is completed by the session when the command has been processed -
 *                  useful for synchronizing when activities should occur in relation to a command
 *                  The only commands that support this presently are SubAck, UnsubAck, PubAck, PubRec and PubComp.
 *                 These completions can be used to signal when processing should continue.
 * @param carry The data to carry though
 * @tparam A The type of data to carry through
 */
final case class Command[A](command: ControlPacket, completed: Option[Promise[Done]], carry: Option[A]) {

  /**
   * JAVA API
   *
   * Send a command to an MQTT session with optional data to carry through
   * into any related event.
   * @param command The command to send
   * @param completed A promise that is completed by the session when the command has been processed -
   *                  useful for synchronizing when activities should occur in relation to a command
   *                  The only commands that support this presently are SubAck, UnsubAck, PubAck, PubRec and PubComp.
   *                 These completions can be used to signal when processing should continue.
   * @param carry The data to carry though
   */
  def this(command: ControlPacket, completed: Optional[CompletionStage[Done]], carry: Optional[A]) =
    this(
      command,
      completed.asScala.map { f =>
        val p = Promise[Done]
        p.future
          .foreach(f.toCompletableFuture.complete)(ExecutionContext.fromExecutorService(ForkJoinPool.commonPool()))
        p
      },
      carry.asScala
    )

  /**
   * Send a command to an MQTT session
   * @param command The command to send
   */
  def this(command: ControlPacket) =
    this(command, None, None)

  /**
   * Send a command to an MQTT session with data to carry through into
   * any related event.
   * @param command The command to send
   * @param carry The data to carry through
   */
  def this(command: ControlPacket, carry: A) =
    this(command, None, Some(carry))
}

object Event {

  /**
   * Receive an event from a MQTT session
   * @param event The event to receive
   * @tparam A The type of data being carried through in general, but not here
   */
  def apply[A](event: ControlPacket): Event[A] =
    new Event(event)

  /**
   * Receive an event from a MQTT session with data to carry through into
   * any related event.
   * @param event The event to receive
   * @param carry The data to carry through
   * @tparam A The type of data to carry through
   */
  def apply[A](event: ControlPacket, carry: A): Event[A] =
    new Event(event, carry)
}

/**
 * Receive an event from a MQTT session with optional data to carry through
 * infrom ay related event.
 * @param event The event to receive
 * @param carry The data to carry though
 * @tparam A The type of data to carry through
 */
final case class Event[A](event: ControlPacket, carry: Option[A]) {

  /**
   * JAVA API
   *
   * Receive an event from a MQTT session with optional data to carry through
   * infrom ay related event.
   * @param event The event to receive
   * @param carry The data to carry though
   */
  def this(event: ControlPacket, carry: Optional[A]) =
    this(event, carry.asScala)

  /**
   * Receive an event from a MQTT session
   * @param event The event to receive
   */
  def this(event: ControlPacket) =
    this(event, None)

  /**
   * Receive an event from a MQTT session with data to carry through into
   * any related event.
   * @param event The event to receive
   * @param carry The data to carry through
   */
  def this(event: ControlPacket, carry: A) =
    this(event, Some(carry))
}

object DecodeErrorOrEvent {

  /**
   * JAVA API
   *
   * Return a Class object representing the carry's type. Java's
   * `.class` method does not do this, and there are many occassions
   * where the generic type needs to be retained.
   * @tparam A The type of the carry
   * @return The `DecodeErrorOrEvent` class including the carry type
   */
  def classOf[A]: Class[DecodeErrorOrEvent[A]] =
    Predef.classOf[DecodeErrorOrEvent[A]]
}

/**
 * JAVA API
 */
final case class DecodeErrorOrEvent[A](v: Either[MqttCodec.DecodeError, Event[A]]) {
  def getDecodeError: Optional[MqttCodec.DecodeError] =
    v match {
      case Right(_) => Optional.empty()
      case Left(de) => Optional.of(de)
    }

  def getEvent: Optional[Event[A]] =
    v match {
      case Right(e) => Optional.of(e)
      case Left(_) => Optional.empty()
    }
}
