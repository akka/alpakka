package akka.stream.alpakka.mqtt.streaming

import akka.Done
import akka.annotation.InternalApi
import akka.util.ByteIterator.ByteArrayIterator
import akka.util.{ByteIterator, ByteString, ByteStringBuilder}

import java.nio.ByteOrder
import java.nio.charset.StandardCharsets
import java.util.Optional
import java.util.concurrent.{CompletionStage, TimeUnit}
import scala.annotation.tailrec
import scala.collection.immutable.IndexedSeq
import scala.concurrent.Promise
import scala.concurrent.duration.{DurationInt, FiniteDuration}

/**
 * 1.5.5 Variable Byte Integer
 * https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html
 */
final case class VariableByteInteger private(underlying: Int) extends AnyVal

/**
 * 1.5.6 Binary Data
 * https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html
 */
final case class BinaryData private(underlying: IndexedSeq[Byte]) extends AnyVal


/**
 * 2 MQTT Control Packet format
 * https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html
 */
sealed abstract class ControlPacket(val packetType: ControlPacketType, val flags: ControlPacketFlags)

/**
 * 2.1.2 MQTT Control Packet type
 * https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html
 */
object ControlPacketType {
  val Reserved:    ControlPacketType = ControlPacketType(0)
  val CONNECT:     ControlPacketType = ControlPacketType(1)
  val CONNACK:     ControlPacketType = ControlPacketType(2)
  val PUBLISH:     ControlPacketType = ControlPacketType(3)
  val PUBACK:      ControlPacketType = ControlPacketType(4)
  val PUBREC:      ControlPacketType = ControlPacketType(5)
  val PUBREL:      ControlPacketType = ControlPacketType(6)
  val PUBCOMP:     ControlPacketType = ControlPacketType(7)
  val SUBSCRIBE:   ControlPacketType = ControlPacketType(8)
  val SUBACK:      ControlPacketType = ControlPacketType(9)
  val UNSUBSCRIBE: ControlPacketType = ControlPacketType(10)
  val UNSUBACK:    ControlPacketType = ControlPacketType(11)
  val PINGREQ:     ControlPacketType = ControlPacketType(12)
  val PINGRESP:    ControlPacketType = ControlPacketType(13)
  val DISCONNECT:  ControlPacketType = ControlPacketType(14)
  val AUTH:        ControlPacketType = ControlPacketType(15)
}
final case class ControlPacketType private (underlying: Int) extends AnyVal


/**
 * 2.1.3 Flags
 * https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html
 */
object ControlPacketFlags {
  val None: ControlPacketFlags = ControlPacketFlags(0)
  val ReservedGeneral: ControlPacketFlags = ControlPacketFlags(0)
  val ReservedPubRel: ControlPacketFlags = ControlPacketFlags(1 << 1)
  val ReservedSubscribe: ControlPacketFlags = ControlPacketFlags(1 << 1)
  val ReservedUnsubscribe: ControlPacketFlags = ControlPacketFlags(1 << 1)
  val ReservedUnsubAck: ControlPacketFlags = ControlPacketFlags(1 << 1)
  val DUP: ControlPacketFlags = ControlPacketFlags(1 << 3)
  val QoSAtMostOnceDelivery: ControlPacketFlags = ControlPacketFlags(0)
  val QoSAtLeastOnceDelivery: ControlPacketFlags = ControlPacketFlags(1 << 1)
  val QoSExactlyOnceDelivery: ControlPacketFlags = ControlPacketFlags(2 << 1)
  val QoSReserved: ControlPacketFlags = ControlPacketFlags(3 << 1)
  val QoSFailure: ControlPacketFlags = ControlPacketFlags(1 << 7)
  val RETAIN: ControlPacketFlags = ControlPacketFlags(1)
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
 * 2.2.1 Packet Identifier https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html
 */
final case class PacketId private (underlying: Int) extends AnyVal


/**
 * 2.2.2 Properties
 * https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html
 */
object PropertyIdentifiers {
  val PayloadFormatIndicator: VariableByteInteger = VariableByteInteger(1)
  val MessageExpiryInterval: VariableByteInteger = VariableByteInteger(2)
  val ContentType: VariableByteInteger = VariableByteInteger(3)
  val ResponseTopic: VariableByteInteger = VariableByteInteger(8)
  val CorrelationData: VariableByteInteger = VariableByteInteger(9)
  val SubscriptionIdentifier: VariableByteInteger = VariableByteInteger(11)
  val SessionExpiryInterval: VariableByteInteger = VariableByteInteger(17)
  val AssignedClientIdentifier: VariableByteInteger = VariableByteInteger(18)
  val ServerKeepAlive: VariableByteInteger = VariableByteInteger(19)
  val AuthenticationMethod: VariableByteInteger = VariableByteInteger(21)
  val AuthenticationData: VariableByteInteger = VariableByteInteger(22)
  val RequestProblemInformation: VariableByteInteger = VariableByteInteger(23)
  val WillDelayInterval: VariableByteInteger = VariableByteInteger(24)
  val RequestResponseInformation: VariableByteInteger = VariableByteInteger(25)
  val ResponseInformation: VariableByteInteger = VariableByteInteger(26)
  val ServerReference: VariableByteInteger = VariableByteInteger(28)
  val ReasonString: VariableByteInteger = VariableByteInteger(31)
  val ReceiveMaximum: VariableByteInteger = VariableByteInteger(33)
  val TopicAliasMaximum: VariableByteInteger = VariableByteInteger(34)
  val TopicAlias: VariableByteInteger = VariableByteInteger(35)
  val MaximumQoS: VariableByteInteger = VariableByteInteger(36)
  val RetainAvailable: VariableByteInteger = VariableByteInteger(37)
  val UserProperty: VariableByteInteger = VariableByteInteger(38)
  val MaximumPacketSize: VariableByteInteger = VariableByteInteger(39)
  val WildcardSubscriptionAvailable: VariableByteInteger = VariableByteInteger(40)
  val SubscriptionIdentifierAvailable: VariableByteInteger = VariableByteInteger(41)
  val SharedSubscriptionAvailable: VariableByteInteger = VariableByteInteger(42)
}


/**
 * 2.4 Reason Code
 * https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html
 */
sealed abstract class ReasonCode (val underlying: Int)
object ReasonCode {
  // Success
  val Success = 0
  val NormalDisconnection = 0
  val GrantedQoS0 = 0
  val GrantedQoS1 = 1
  val GrantedQoS2 = 2
  val DisconnectWithWillMessage = 4
  val NoMatchingSubscribers = 16
  val NoSubscriptionExisted = 17
  val ContinueAuthentication = 24
  val ReAuthenticate = 25
  // Failure
  val UnspecifiedError = 128
  val MalformedPacket = 129
  val ProtocolError = 130
  val ImplementationSpecificError = 131
  val UnsupportedProtocolVersion = 132
  val ClientIdentifierNotValid = 133
  val BadUserNameOrPassword = 134
  val NotAuthorized = 135
  val ServerUnavailable = 136
  val ServerBusy = 137
  val Banned = 138
  val ServerShuttingDown = 139
  val BadAuthenticationMethod = 140
  val KeepAliveTimeout = 141
  val SessionTakeOver = 142
  val TopicFilterInvalid = 143
  val TopicNameInvalid = 144
  val PacketIdentifierInUse = 145
  val PacketIdentifierNotFound = 146
  val ReceiveMaximumExceeded = 147
  val TopicAliasInvalid = 148
  val PacketTooLarge = 149
  val MessageRateTooHigh = 150
  val QuotaExceeded = 151
  val AdministrativeAction = 152
  val PayloadFormatInvalid = 153
  val RetainNotSupported = 154
  val QoSNotSupported = 155
  val UseAnotherServer = 156
  val ServerMoved = 157
  val SharedSubscriptionsNotSupported = 158
  val ConnectionRateExceeded = 159
  val MaximumConnectTime = 160
  val SubscriptionIdentifiersNotSupported = 161
  val WildcardSubscriptionsNotSupported = 162
}


case object Reserved extends ControlPacket(ControlPacketType.Reserved, ControlPacketFlags.ReservedGeneral)

/**
 * 3.1 CONNECT – Client requests a connection to a Server
 * https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html
 */
final case class Connect(protocolName: Connect.ProtocolName,
                         protocolVersion: Connect.ProtocolLevel,
                         connectFlags: ConnectFlags,
                         keepAlive: FiniteDuration,
                         properties: ConnectProperties,
                         payload: ConnectPayload)
  extends ControlPacket(ControlPacketType.CONNECT, ControlPacketFlags.ReservedGeneral)
object Connect {
  type ProtocolName = String
  val Mqtt: ProtocolName = "MQTT"

  type ProtocolLevel = Int
  val v311: ProtocolLevel = 4
  val v5: ProtocolLevel = 5

  val DefaultKeepAlive: FiniteDuration =
    60.seconds

  /**
   * Create connect object.
   */
  def apply(clientId: String,
            protocolVersion: ProtocolLevel = v5,
            connectProperties: ConnectProperties = ConnectProperties(),
            username: Option[String] = None,
            password: Option[String] = None,
            keepAlive: FiniteDuration = DefaultKeepAlive,
            cleanStart: Boolean = false,
            willRetain: Boolean = false,
            willQoS: Int = 0,
            lastWillMessage: Option[LastWillMessage] = None
           ): Connect = {
    val flags = {
      import ConnectFlags._
      (if (username.isDefined) UsernameFlag else None) |
        (if (password.isDefined) PasswordFlag else None) |
        (if (cleanStart) CleanStart else None) |
        (if (willRetain) WillRetain else None) |
        (willQoS match {
          case 0 => WillQoSAtMostOnceDelivery
          case 1 => WillQoSAtLeastOnceDelivery
          case 2 => WillQoSExactlyOnceDelivery
          case _ => None
        }) |
        (if (lastWillMessage.isDefined) WillFlag else None)
    }
    val payload = ConnectPayload(
      clientId,
      lastWillMessage.map(_.willProperties),
      lastWillMessage.map(_.willTopic),
      lastWillMessage.map(_.willPayload),
      username,
      password
    )

    new Connect(Mqtt, protocolVersion, flags, keepAlive, connectProperties, payload)
  }

}


/**
 * 3.1.2.3 Connect Flags
 * https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html
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
object ConnectFlags {
  val None: ConnectFlags = ConnectFlags(0)
  val Reserved: ConnectFlags = ConnectFlags(1)
  val CleanStart: ConnectFlags = ConnectFlags(1 << 1)
  val WillFlag: ConnectFlags = ConnectFlags(1 << 2)
  val WillQoSAtMostOnceDelivery: ConnectFlags = ConnectFlags(0)
  val WillQoSAtLeastOnceDelivery: ConnectFlags = ConnectFlags(1 << 3)
  val WillQoSExactlyOnceDelivery: ConnectFlags = ConnectFlags(1 << 4)
  val WillRetain: ConnectFlags = ConnectFlags(1 << 5)
  val PasswordFlag: ConnectFlags = ConnectFlags(1 << 6)
  val UsernameFlag: ConnectFlags = ConnectFlags(1 << 7)
}


/**
 * 3.1.2.11 Connect Properties
 * https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html
 */
final case class ConnectProperties(
                                    SessionExpiryInterval: Option[Int] = None,
                                    ReceiveMaximum: Option[Int] = None,
                                    MaximumPacketSize: Option[Int] = None,
                                    TopicAliasMaximum: Option[Int] = None,
                                    RequestResponseInformation: Option[Int] = None,
                                    RequestProblemInformation: Option[Int] = None,
                                    UserProperties: Option[List[(String, String)]] = None,
                                    AuthenticationMethod: Option[String] = None,
                                    AuthenticationData: Option[BinaryData] = None
                                  ) {
  def this() = this(None, None, None, None, None, None, None, None, None)
}
object ConnectProperties {
  def apply(): ConnectProperties = new ConnectProperties()
}

/**
 * 3.1.3 Connect Payload
 * https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html
 */
final case class ConnectPayload(
                                 ClientIdentifier: String,
                                 WillProperties: Option[ConnectPayloadWillProperties],
                                 WillTopic: Option[String],
                                 WillPayload: Option[BinaryData],
                                 UserName: Option[String],
                                 Password: Option[String]
                               )


object ConnectPayloadWillProperties {
  def apply(): ConnectPayloadWillProperties = new ConnectPayloadWillProperties()
}

/**
 * 3.1.3.2 Will Properties
 * https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html
 */
final case class ConnectPayloadWillProperties(
                                               WillDelayInterval: Option[Int],
                                               PayloadFormatIndicator: Option[Int],
                                               MessageExpiryInterval: Option[Int],
                                               ContentType: Option[String],
                                               ResponseTopic: Option[String],
                                               CorrelationData: Option[BinaryData],
                                               UserProperties: Option[List[(String, String)]],
                                             ) {
  def this() = this(None, None, None, None, None, None, None)
}

final case class LastWillMessage(
                                  willProperties: ConnectPayloadWillProperties,
                                  willTopic: String,
                                  willPayload: BinaryData
                                )


/**
 * 3.2 CONNACK – Connect acknowledgement
 * https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html
 */
final case class ConnAck(connAckFlags: ConnAckFlags, reasonCode: ConnAckReasonCode, properties: ConnAckProperties)
  extends ControlPacket(ControlPacketType.CONNACK, ControlPacketFlags.ReservedGeneral)

object ConnAckFlags {
  val None: ConnAckFlags = ConnAckFlags(0)
  val SessionPresent: ConnAckFlags = ConnAckFlags(1)
}


/**
 * 3.2.2.3 CONNACK Properties
 * https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html
 */
final case class ConnAckProperties(
                                    SessionExpiryInterval: Option[Int] = None,
                                    ReceiveMaximum: Option[Int] = None,
                                    MaximumQoS: Option[Int] = None,
                                    RetainAvailable: Option[Int] = None,
                                    MaximumPacketSize: Option[Int] = None,
                                    AssignedClientIdentifier: Option[String] = None,
                                    TopicAliasMaximum: Option[Int] = None,
                                    ReasonString: Option[String] = None,
                                    UserProperties: Option[List[(String, String)]] = None,
                                    WildcardSubscriptionAvailable: Option[Int] = None,
                                    SharedSubscriptionAvailable: Option[Int] = None,
                                    SubscriptionIdentifierAvailable: Option[Int] = None,
                                    ServerKeepAlive: Option[Int] = None,
                                    ResponseInformation: Option[String] = None,
                                    ServerReference: Option[String] = None,
                                    AuthenticationMethod: Option[String] = None,
                                    AuthenticationData: Option[BinaryData] = None
                                  )
object ConnAckProperties {
  def apply(): ConnAckProperties = new ConnAckProperties()
}

/**
 * 3.2.2.1 Connect Acknowledge Flags
 * https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html
 */
final case class ConnAckFlags private (underlying: Int) extends AnyVal

object ConnAckReasonCode {
  val Success: ConnAckReasonCode = ConnAckReasonCode(ReasonCode.Success)
  val UnspecifiedError: ConnAckReasonCode = ConnAckReasonCode(ReasonCode.UnspecifiedError)
  val MalformedPacket: ConnAckReasonCode = ConnAckReasonCode(ReasonCode.MalformedPacket)
  val ProtocolError: ConnAckReasonCode = ConnAckReasonCode(ReasonCode.ProtocolError)
  val ImplementationSpecificError: ConnAckReasonCode = ConnAckReasonCode(ReasonCode.ImplementationSpecificError)
  val UnsupportedProtocolVersion: ConnAckReasonCode = ConnAckReasonCode(ReasonCode.UnsupportedProtocolVersion)
  val ClientIdentifierNotValid: ConnAckReasonCode = ConnAckReasonCode(ReasonCode.ClientIdentifierNotValid)
  val BadUserNameOrPassword: ConnAckReasonCode = ConnAckReasonCode(ReasonCode.BadUserNameOrPassword)
  val NotAuthorized: ConnAckReasonCode = ConnAckReasonCode(ReasonCode.NotAuthorized)
  val ServerUnavailable: ConnAckReasonCode = ConnAckReasonCode(ReasonCode.ServerUnavailable)
  val ServerBusy: ConnAckReasonCode = ConnAckReasonCode(ReasonCode.ServerBusy)
  val Banned: ConnAckReasonCode = ConnAckReasonCode(ReasonCode.Banned)
  val BadAuthenticationMethod: ConnAckReasonCode = ConnAckReasonCode(ReasonCode.BadAuthenticationMethod)
  val TopicNameInvalid: ConnAckReasonCode = ConnAckReasonCode(ReasonCode.TopicNameInvalid)
  val PacketTooLarge: ConnAckReasonCode = ConnAckReasonCode(ReasonCode.PacketTooLarge)
  val QuotaExceeded: ConnAckReasonCode = ConnAckReasonCode(ReasonCode.QuotaExceeded)
  val PayloadFormatInvalid: ConnAckReasonCode = ConnAckReasonCode(ReasonCode.PayloadFormatInvalid)
  val RetainNotSupported: ConnAckReasonCode = ConnAckReasonCode(ReasonCode.RetainNotSupported)
  val QoSNotSupported: ConnAckReasonCode = ConnAckReasonCode(ReasonCode.QoSNotSupported)
  val UseAnotherServer: ConnAckReasonCode = ConnAckReasonCode(ReasonCode.UseAnotherServer)
  val ServerMoved: ConnAckReasonCode = ConnAckReasonCode(ReasonCode.ServerMoved)
  val ConnectionRateExceeded: ConnAckReasonCode = ConnAckReasonCode(ReasonCode.ConnectionRateExceeded)
}

final case class ConnAckReasonCode(override val underlying: Int) extends ReasonCode(underlying)



/**
 * 3.3 PUBLISH – Publish message
 * https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os
 */
final case class Publish @InternalApi private[streaming] (override val flags: ControlPacketFlags,
                                                          topicName: String,
                                                          packetId: Option[PacketId],
                                                          properties: PublishProperties,
                                                          payload: ByteString)
  extends ControlPacket(ControlPacketType.PUBLISH, flags) {

  def this(topicName: String, payload: ByteString) =
    this(ControlPacketFlags.QoSAtMostOnceDelivery, topicName, None, PublishProperties(), payload)

  def this(flags: ControlPacketFlags, topicName: String, payload: ByteString) =
    this(flags, topicName, None, PublishProperties(), payload)

  def this(flags: ControlPacketFlags, topicName: String, properties: PublishProperties, payload: ByteString) =
    this(flags, topicName, None, properties, payload)
}
object Publish {
  def apply(topicName: String, payload: ByteString): Publish =
    new Publish(topicName, payload)

  def apply(flags: ControlPacketFlags, topicName: String, payload: ByteString): Publish =
    new Publish(flags, topicName, payload)

  def apply(flags: ControlPacketFlags, topicName: String, properties: PublishProperties, payload: ByteString): Publish =
    new Publish(flags, topicName, properties, payload)
}

/**
 * 3.3.2.3 PUBLISH Properties
 * https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html
 */
final case class PublishProperties(
                                    PayloadFormatIndicator: Option[Int] = None,
                                    MessageExpiryInterval: Option[Int] = None,
                                    TopicAlias: Option[Int] = None,
                                    ResponseTopic: Option[String] = None,
                                    CorrelationData: Option[BinaryData] = None,
                                    UserProperties: Option[List[(String, String)]] = None,
                                    SubscriptionIdentifier: Option[Int] = None,
                                    ContentType: Option[String] = None,
                                  ) {
  def this() = this(None, None, None, None, None, None, None, None)
}
object PublishProperties{
  def apply() = new PublishProperties()
}


/**
 * 3.4 PUBACK - Publish acknowledgement
 * https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os
 */
final case class PubAck(packetId: PacketId, reasonCode: PubAckReasonCode, properties: Option[PubAckProperties])
  extends ControlPacket(ControlPacketType.PUBACK, ControlPacketFlags.ReservedGeneral)


/**
 * 3.4.1.2 PUBACK Reason Code
 */
final case class PubAckReasonCode(override val underlying: Int) extends ReasonCode(underlying)
object PubAckReasonCode {
  val Success: PubAckReasonCode = PubAckReasonCode(ReasonCode.Success)
  val NoMatchingSubscribers: PubAckReasonCode = PubAckReasonCode(ReasonCode.NoMatchingSubscribers)
  val UnspecifiedError: PubAckReasonCode = PubAckReasonCode(ReasonCode.UnspecifiedError)
  val ImplementationSpecificError: PubAckReasonCode = PubAckReasonCode(ReasonCode.ImplementationSpecificError)
  val NotAuthorized: PubAckReasonCode = PubAckReasonCode(ReasonCode.NotAuthorized)
  val TopicNameInvalid: PubAckReasonCode = PubAckReasonCode(ReasonCode.TopicNameInvalid)
  val PacketIdentifierInUse: PubAckReasonCode = PubAckReasonCode(ReasonCode.PacketIdentifierInUse)
  val QuotaExceeded: PubAckReasonCode = PubAckReasonCode(ReasonCode.QuotaExceeded)
  val PayloadFormatInvalid: PubAckReasonCode = PubAckReasonCode(ReasonCode.PayloadFormatInvalid)
}


/**
 * 3.4.2.2 PUBACK Properties
 * https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-o
 */
final case class PubAckProperties(
                                   ReasonString: Option[String] = None,
                                   UserProperties: Option[List[(String, String)]] = None
                                 )
object PubAckProperties {
  def apply(): PubAckProperties = PubAckProperties(None, None)
}

/**
 * 3.5 PUBREC - Publish received
 * https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os
 */
final case class PubRec(packetId: PacketId, reasonCode: PubRecReasonCode, properties: Option[PubRecProperties])
  extends ControlPacket(ControlPacketType.PUBREC, ControlPacketFlags.ReservedGeneral)

/**
 * 3.5.2.1 PUBREC Reason Code
 * https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-o
 */
final case class PubRecReasonCode(override val underlying: Int) extends ReasonCode(underlying)
object PubRecReasonCode {
  val Success: PubRecReasonCode = PubRecReasonCode(ReasonCode.Success)
  val NoMatchingSubscribers: PubRecReasonCode = PubRecReasonCode(ReasonCode.NoMatchingSubscribers)
  val UnspecifiedError: PubRecReasonCode = PubRecReasonCode(ReasonCode.UnspecifiedError)
  val ImplementationSpecificError: PubRecReasonCode = PubRecReasonCode(ReasonCode.ImplementationSpecificError)
  val NotAuthorized: PubRecReasonCode = PubRecReasonCode(ReasonCode.NotAuthorized)
  val TopicNameInvalid: PubRecReasonCode = PubRecReasonCode(ReasonCode.TopicNameInvalid)
  val PacketIdentifierInUse: PubRecReasonCode = PubRecReasonCode(ReasonCode.PacketIdentifierInUse)
  val QuotaExceeded: PubRecReasonCode = PubRecReasonCode(ReasonCode.QuotaExceeded)
  val PayloadFormatInvalid: PubRecReasonCode = PubRecReasonCode(ReasonCode.PayloadFormatInvalid)
}

/**
 * 3.5.2.2 PUBREC Properties
 * https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-o
 */
final case class PubRecProperties(
                                   ReasonString: Option[String],
                                   UserProperties: Option[List[(String, String)]]
                                 )
object PubRecProperties {
  def apply(): PubRecProperties = PubRecProperties(None, None)
}


/**
 * 3.6 PUBREL - Publish Release
 * https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-o
 */
final case class PubRel(packetId: PacketId, reasonCode: PubRelReasonCode, properties: Option[PubRelProperties])
  extends ControlPacket(ControlPacketType.PUBREL, ControlPacketFlags.ReservedPubRel)

/**
 * 3.6.2.1 PUBREL Reason Code
 * https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-o
 */
final case class PubRelReasonCode(override val underlying: Int) extends ReasonCode(underlying)
object PubRelReasonCode {
  val Success: PubRelReasonCode = PubRelReasonCode(ReasonCode.Success)
  val PacketIdentifierNotFound: PubRelReasonCode = PubRelReasonCode(ReasonCode.PacketIdentifierNotFound)
}

/**
 * 3.6.2.2 PUBREL Properties
 * https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-o
 */
final case class PubRelProperties(
                                   ReasonString: Option[String],
                                   UserProperties: Option[List[(String, String)]]
                                 )
object PubRelProperties {
  def apply(): PubRelProperties = PubRelProperties(None, None)
}

/**
 * 3.7 PUBCOMP - Publish Complete
 * https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-o
 */
final case class PubComp(packetId: PacketId, reasonCode: PubCompReasonCode, properties: Option[PubCompProperties])
  extends ControlPacket(ControlPacketType.PUBCOMP, ControlPacketFlags.ReservedGeneral)

/**
 * 3.7.2.1 PUBCOMP Reason Code
 * https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-o
 */
final case class PubCompReasonCode(override val underlying: Int) extends ReasonCode(underlying)
object PubCompReasonCode {
  val Success: PubCompReasonCode = PubCompReasonCode(ReasonCode.Success)
  val PacketIdentifierNotFound: PubCompReasonCode = PubCompReasonCode(ReasonCode.PacketIdentifierNotFound)
}

/**
 * 3.7.2.2 PUBCOMP Properties
 * https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-o
 */
final case class PubCompProperties(
                                    ReasonString: Option[String],
                                    UserProperties: Option[List[(String, String)]]
                                  )
object PubCompProperties {
  def apply(): PubCompProperties = PubCompProperties(None, None)
}

object Subscribe {

  def apply(topicFilter: String): Subscribe =
    new Subscribe(topicFilter)

  def apply(topicFilter: (String, SubscribeOptions)): Subscribe =
    new Subscribe(topicFilter)

  def apply(topicFilters: Seq[(String, SubscribeOptions)]): Subscribe =
    new Subscribe(topicFilters)

  def apply(properties: SubscribeProperties, topicFilter: (String, SubscribeOptions)): Subscribe =
    new Subscribe(properties, topicFilter)

  def apply(properties: SubscribeProperties, topicFilters: Seq[(String, SubscribeOptions)]): Subscribe =
    new Subscribe(properties, topicFilters)

}

/**
 * 3.8 SUBSCRIBE - Subscribe to topics
 * https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os
 */
final case class Subscribe @InternalApi private[streaming] (packetId: PacketId,
                                                            properties: SubscribeProperties,
                                                            topicFilters: Seq[(String, SubscribeOptions)])
  extends ControlPacket(ControlPacketType.SUBSCRIBE, ControlPacketFlags.ReservedSubscribe) {

  def this(topicFilter: String) =
    this(PacketId(0), SubscribeProperties(), Seq(topicFilter -> SubscribeOptions.None))

  def this(topicFilter: (String, SubscribeOptions)) =
    this(PacketId(0), SubscribeProperties(), Seq(topicFilter))

  def this(topicFilters: Seq[(String, SubscribeOptions)]) =
    this(PacketId(0), SubscribeProperties(), topicFilters)

  def this(properties: SubscribeProperties, topicFilter: (String, SubscribeOptions)) =
    this(PacketId(0), properties, Seq(topicFilter))

  def this(properties: SubscribeProperties, topicFilters: Seq[(String, SubscribeOptions)]) =
    this(PacketId(0), properties, topicFilters)
}

/**
 * 3.8.3.1 Subscription Options
 * https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os
 */
final case class SubscribeOptions private(underlying: Int) extends AnyVal {
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
object SubscribeOptions {
  val None: SubscribeOptions = SubscribeOptions(0)
  val RetainHandlingReserved: SubscribeOptions = SubscribeOptions(3 << 4)
  val RetainHandling0: SubscribeOptions = SubscribeOptions(0)
  val RetainHandling1: SubscribeOptions = SubscribeOptions(1 << 4)
  val RetainHandling2: SubscribeOptions = SubscribeOptions(2 << 4)
  val RetainAsPublished: SubscribeOptions = SubscribeOptions(1 << 3)
  val NoLocal: SubscribeOptions = SubscribeOptions(1 << 2)
  val QoSReserved: SubscribeOptions = SubscribeOptions(3)
  val QoSAtMostOnceDelivery: SubscribeOptions = SubscribeOptions(0)
  val QoSAtLeastOnceDelivery: SubscribeOptions = SubscribeOptions(1)
  val QoSExactlyOnceDelivery: SubscribeOptions = SubscribeOptions(2)
}

/**
 * 3.8.2.1 SUBSCRIBE Properties
 * https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os
 */
final case class SubscribeProperties(
                                      SubscriptionIdentifier: Option[Int] = None,
                                      UserProperties: Option[List[(String, String)]] = None
                                    )
object SubscribeProperties {
  def apply() = new SubscribeProperties()
}


/**
 * 3.9 SUBACK – Subscribe acknowledgement
 * https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os
 */
final case class SubAck(packetId: PacketId, properties: SubAckProperties, reasonCodes: Seq[SubAckReasonCode])
  extends ControlPacket(ControlPacketType.SUBACK, ControlPacketFlags.ReservedGeneral)

/**
 * 3.9.2.1 SUBSCRIBE Properties
 * https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os
 */
final case class SubAckProperties(
                                   ReasonString: Option[String] = None,
                                   UserProperties: Option[List[(String, String)]] = None
                                 )
object SubAckProperties {
  def apply() = new SubAckProperties()
}


/**
 * 3.9.3 SUBACK Payload (List of Reason codes)
 * https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-o
 */
final case class SubAckReasonCode(override val underlying: Int) extends ReasonCode(underlying)
object SubAckReasonCode {
  val GrantedQoS0: SubAckReasonCode = SubAckReasonCode(ReasonCode.GrantedQoS0)
  val GrantedQoS1: SubAckReasonCode = SubAckReasonCode(ReasonCode.GrantedQoS1)
  val GrantedQoS2: SubAckReasonCode = SubAckReasonCode(ReasonCode.GrantedQoS2)
  val UnspecifiedError: SubAckReasonCode = SubAckReasonCode(ReasonCode.UnspecifiedError)
  val ImplementationSpecificError: SubAckReasonCode = SubAckReasonCode(ReasonCode.ImplementationSpecificError)
  val NotAuthorized: SubAckReasonCode = SubAckReasonCode(ReasonCode.NotAuthorized)
  val TopicFilterInvalid: SubAckReasonCode = SubAckReasonCode(ReasonCode.TopicFilterInvalid)
  val PacketIdentifierInUse: SubAckReasonCode = SubAckReasonCode(ReasonCode.PacketIdentifierNotFound)
  val QuotaExceeded: SubAckReasonCode = SubAckReasonCode(ReasonCode.QuotaExceeded)
  val SharedSubscriptionsNotSupported: SubAckReasonCode = SubAckReasonCode(ReasonCode.SharedSubscriptionsNotSupported)
  val SubscriptionIdentifiersNotSupported: SubAckReasonCode = SubAckReasonCode(ReasonCode.SubscriptionIdentifiersNotSupported)
  val WildcardSubscriptionsNotSupported: SubAckReasonCode = SubAckReasonCode(ReasonCode.WildcardSubscriptionsNotSupported)
}


/**
 * 3.10 UNSUBSCRIBE – Unsubscribe from topics
 * https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-o
 */
final case class Unsubscribe @InternalApi private[streaming] (packetId: PacketId, properties: UnsubscribeProperties, topicFilters: Seq[String])
  extends ControlPacket(ControlPacketType.UNSUBSCRIBE, ControlPacketFlags.ReservedUnsubscribe) {

  def this(topicFilter: String) =
    this(PacketId(0), UnsubscribeProperties(), Seq(topicFilter))

  def this(topicFilters: Seq[String]) =
    this(PacketId(0), UnsubscribeProperties(), topicFilters)

  def this(properties: UnsubscribeProperties, topicFilter: String) =
    this(PacketId(0), properties, Seq(topicFilter))

  def this(properties: UnsubscribeProperties, topicFilters: Seq[String]) =
    this(PacketId(0), properties, topicFilters)
}
object Unsubscribe {
  def apply(topicFilter: String): Unsubscribe =
    new Unsubscribe(topicFilter)

  def apply(topicFilters: Seq[String]): Unsubscribe =
    new Unsubscribe(topicFilters)

  def apply(properties: UnsubscribeProperties, topicFilter: String): Unsubscribe =
    new Unsubscribe(properties, topicFilter)

  def apply(properties: UnsubscribeProperties, topicFilters: Seq[String]): Unsubscribe =
    new Unsubscribe(properties, topicFilters)
}

/**
 * 3.10.2.1 UNSUBSCRIBE Properties
 * https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os
 */
final case class UnsubscribeProperties(
                                        UserProperties: Option[List[(String, String)]] = None
                                      )
object UnsubscribeProperties {
  def apply() = new UnsubscribeProperties()
}

/**
 * 3.11 UNSUBACK – Subscribe acknowledgement
 * https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os
 */
final case class UnsubAck(packetId: PacketId, properties: UnsubAckProperties, reasonCodes: Seq[UnsubAckReasonCode])
  extends ControlPacket(ControlPacketType.UNSUBACK, ControlPacketFlags.ReservedUnsubAck)

/**
 * 3.11.2.1 UNSUBACK Properties
 * https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os
 */
final case class UnsubAckProperties(
                                     ReasonString: Option[String] = None,
                                     UserProperties: Option[List[(String, String)]] = None
                                   )
object UnsubAckProperties {
  def apply() = new UnsubAckProperties()
}

/**
 * 3.11.3 UNSUBACK Payload (List of Reason codes)
 * https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-o
 */
final case class UnsubAckReasonCode(override val underlying: Int) extends ReasonCode(underlying)
object UnsubAckReasonCode {
  val Success: UnsubAckReasonCode = UnsubAckReasonCode(ReasonCode.Success)
  val NoSubscriptionExisted: UnsubAckReasonCode = UnsubAckReasonCode(ReasonCode.NoSubscriptionExisted)
  val UnspecifiedError: UnsubAckReasonCode = UnsubAckReasonCode(ReasonCode.UnspecifiedError)
  val ImplementationSpecifigError: UnsubAckReasonCode = UnsubAckReasonCode(ReasonCode.ImplementationSpecificError)
  val NotAuthorized: UnsubAckReasonCode = UnsubAckReasonCode(ReasonCode.NotAuthorized)
  val TopicFilterInvalid: UnsubAckReasonCode = UnsubAckReasonCode(ReasonCode.TopicFilterInvalid)
  val PacketIdentifierInUse: UnsubAckReasonCode = UnsubAckReasonCode(ReasonCode.PacketIdentifierNotFound)
}

/**
 * 3.12 PINGREQ – PING request
 * https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html
 */
case object PingReq extends ControlPacket(ControlPacketType.PINGREQ, ControlPacketFlags.ReservedGeneral)

/**
 * 3.13 PINGRESP – PING response
 * https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html
 */
case object PingResp extends ControlPacket(ControlPacketType.PINGRESP, ControlPacketFlags.ReservedGeneral)


/**
 * 3.14 DISCONNECT – Disconnect notification
 * https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-o
 */
final case class Disconnect(reasonCode: DisconnectReasonCode, properties: DisconnectProperties)
  extends ControlPacket(ControlPacketType.DISCONNECT, ControlPacketFlags.ReservedGeneral) {
  def this() = this(DisconnectReasonCode.NormalDisconnection, DisconnectProperties())
}
object Disconnect {
  def apply() = new Disconnect()
}

/**
 * 3.14.2.1 DISCONNECT Reason Code
 * https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-o
 */
final case class DisconnectReasonCode(override val underlying: Int) extends ReasonCode(underlying)
object DisconnectReasonCode {
  val NormalDisconnection: DisconnectReasonCode = DisconnectReasonCode(ReasonCode.NormalDisconnection)
  val DisconnectWithWillMessage: DisconnectReasonCode = DisconnectReasonCode(ReasonCode.DisconnectWithWillMessage)
  val UnspecifiedError: DisconnectReasonCode = DisconnectReasonCode(ReasonCode.UnspecifiedError)
  val MalformedPacket: DisconnectReasonCode = DisconnectReasonCode(ReasonCode.MalformedPacket)
  val ProtocolError: DisconnectReasonCode = DisconnectReasonCode(ReasonCode.ProtocolError)
  val ImplementationSpecificError: DisconnectReasonCode = DisconnectReasonCode(ReasonCode.ImplementationSpecificError)
  val NotAuthorized: DisconnectReasonCode = DisconnectReasonCode(ReasonCode.NotAuthorized)
  val ServerBusy: DisconnectReasonCode = DisconnectReasonCode(ReasonCode.ServerBusy)
  val ServerShuttingDown: DisconnectReasonCode = DisconnectReasonCode(ReasonCode.ServerShuttingDown)
  val KeepAliveTimout: DisconnectReasonCode = DisconnectReasonCode(ReasonCode.KeepAliveTimeout)
  val SessionTakeOver: DisconnectReasonCode = DisconnectReasonCode(ReasonCode.SessionTakeOver)
  val TopicFilerInvalid: DisconnectReasonCode = DisconnectReasonCode(ReasonCode.TopicFilterInvalid)
  val TopicNameInvalid: DisconnectReasonCode = DisconnectReasonCode(ReasonCode.TopicNameInvalid)
  val PacketTooLarge: DisconnectReasonCode = DisconnectReasonCode(ReasonCode.PacketTooLarge)
  val MessageRateTooHigh: DisconnectReasonCode = DisconnectReasonCode(ReasonCode.MessageRateTooHigh)
  val QuotaExceeded: DisconnectReasonCode = DisconnectReasonCode(ReasonCode.QuotaExceeded)
  val AdministrativeAction: DisconnectReasonCode = DisconnectReasonCode(ReasonCode.AdministrativeAction)
  val PayloadFormatInvalid: DisconnectReasonCode = DisconnectReasonCode(ReasonCode.PayloadFormatInvalid)
  val RetainNotSupported: DisconnectReasonCode = DisconnectReasonCode(ReasonCode.RetainNotSupported)
  val QoSNotSupported: DisconnectReasonCode = DisconnectReasonCode(ReasonCode.QoSNotSupported)
  val UseAnotherServer: DisconnectReasonCode = DisconnectReasonCode(ReasonCode.UseAnotherServer)
  val ServerMoved: DisconnectReasonCode = DisconnectReasonCode(ReasonCode.ServerMoved)
  val SharedSubscriptionsNotSupported: DisconnectReasonCode = DisconnectReasonCode(ReasonCode.SharedSubscriptionsNotSupported)
  val ConnectionRateExceeded: DisconnectReasonCode = DisconnectReasonCode(ReasonCode.ConnectionRateExceeded)
  val MaximumConnectTime: DisconnectReasonCode = DisconnectReasonCode(ReasonCode.MaximumConnectTime)
  val SubscriptionIdentifiersNotSupported: DisconnectReasonCode = DisconnectReasonCode(ReasonCode.SubscriptionIdentifiersNotSupported)
  val WildcardSubscriptionsNotSupported: DisconnectReasonCode = DisconnectReasonCode(ReasonCode.WildcardSubscriptionsNotSupported)
}

/**
 * 3.14.2.2 DISCONNECT Properties
 * https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os
 */
final case class DisconnectProperties(
                                       SessionExpiryInterval: Option[Int] = None,
                                       ReasonString: Option[String] = None,
                                       UserProperties: Option[List[(String, String)]] = None,
                                       ServerReference: Option[String] = None
                                     )
object DisconnectProperties {
  def apply() = new DisconnectProperties()
}

/**
 * 3.15 AUTH – Authentication Exchange
 * https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-o
 */
final case class Auth(reasonCode: AuthReasonCode, properties: AuthProperties)
  extends ControlPacket(ControlPacketType.AUTH, ControlPacketFlags.ReservedGeneral)

/**
 * 3.15.2.1 Auth Reason Code
 * https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-o
 */
final case class AuthReasonCode(override val underlying: Int) extends ReasonCode(underlying)
object AuthReasonCode {
  val Success: AuthReasonCode = AuthReasonCode(ReasonCode.Success)
  val ContinueAuthentication: AuthReasonCode = AuthReasonCode(ReasonCode.ContinueAuthentication)
  val ReAuthenticate: AuthReasonCode = AuthReasonCode(ReasonCode.ReAuthenticate)
}

/**
 * 3.15.2.2 AUTH Properties
 * https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os
 */
final case class AuthProperties(
                                 AuthenticationMethod: String = "",
                                 AuthenticationData: Option[BinaryData] = None,
                                 ReasonString: Option[String] = None,
                                 UserProperties: Option[List[(String, String)]] = None
                               )




object MqttCodec {

  // TODO Following Packet Serialization/Deserialization Methods are not yet updated
  //        PUBREC
  //        PUBREL
  //        PUBCOMP
  //        UNSUBSCRIBE
  //        UNSUBACK

  private implicit val byteOrder: ByteOrder = ByteOrder.BIG_ENDIAN

  /**
   * Fail when trying to encode a packet which cannot be express in the protocol Version
   */
  def protocolLevelInsufficient(protocolLevel: Connect.ProtocolLevel, expectedProtocolLevel: Connect.ProtocolLevel, failMessage: String): Unit =
    throw new Exception(s"$failMessage, Actual ProtocolLevel: $protocolLevel, Expected ProtocolLevel: $expectedProtocolLevel")

  /**
   * Returned by decoding when no decoding can be performed
   */
  sealed abstract class DecodeError

  /**
   * Returned by decoding if Protocol Error occured.
   */
  sealed abstract class ProtocolError extends DecodeError

  /**
   * Returned by decoding if invalid payload format indicator was given
   */
  sealed abstract class PayloadFormatInvalid extends DecodeError

  /**
   * Not enough bytes in the byte iterator
   */
  final case object BufferUnderflow extends DecodeError

  /**
   * Cannot determine the type/flags combination of the control packet
   */
  final case class UnknownPacketType(packetType: ControlPacketType, flags: ControlPacketFlags) extends DecodeError

  /**
   *
   */
  final case class UnknownPropertyId(integer: VariableByteInteger) extends DecodeError

  /**
   * A message has been received that exceeds the maximum we have chosen--which is
   * typically much less than what the spec permits. The reported sizes do not
   * include the fixed header size of 2 bytes.
   */
  final case class InvalidPacketSize(packetSize: Int, maxPacketSize: Int) extends DecodeError


  /**
   * Cannot determine the protocol name/level combination of the connect
   */
  final case class UnknownConnectProtocol(protocolName: Either[DecodeError, String], protocolLevel: Connect.ProtocolLevel)
    extends DecodeError

  /**
   * Bit 0 of the connect flag was set - which it should not be as it is reserved.
   */
  final case object ConnectFlagReservedSet extends DecodeError

  /**
   * Invalid QoS Flag specified
   */
  final case object InvalidQoSFlag extends DecodeError

  /**
   * Invalid Retain Handling Flag specified
   */
  final case object InvalidRetainHandlingFlag extends ProtocolError

  /**
   * Something is wrong with the connect message
   */
  final case class BadConnectMessage(subDecodeError: DecodeError) extends DecodeError
  final case class BadConnectProperties(subDecodeError: DecodeError) extends DecodeError
  final case class BadConnectPayload(subDecodeError: DecodeError) extends DecodeError
  final case class BadConnectPayloadWillProperties(subDecodeError: DecodeError) extends DecodeError

  /**
   * Something is wrong with the connack message
   */
  final case class BadConnAckMessage(subDecodeError: DecodeError) extends DecodeError
  final case class BadConnAckProperties(subDecodeError: DecodeError) extends DecodeError

  /**
   * Something is wrong with the publish message
   */
  final case class BadPublishMessage(subDecodeError: DecodeError) extends DecodeError
  final case class BadPublishProperties(subDecodeError: DecodeError) extends DecodeError

  /**
   * Something is wrong with the publish acknowledgement message
   */
  final case class BadPubAckMessage(subDecodeError: DecodeError) extends DecodeError
  final case class BadPubAckProperties(subDecodeError: DecodeError) extends DecodeError

  /**
   * Something is wrong with the publish received message
   */
  final case class BadPubRecMessage(subDecodeError: DecodeError) extends DecodeError
  final case class BadPubRecProperties(subDecodeError: DecodeError) extends DecodeError

  /**
   * Something is wrong with the publish release message
   */
  final case class BadPubRelMessage(subDecodeError: DecodeError) extends DecodeError
  final case class BadPubRelProperties(subDecodeError: DecodeError) extends DecodeError

  /**
   * Something is wrong with the publish complete message
   */
  final case class BadPubCompMessage(subDecodeError: DecodeError) extends DecodeError
  final case class BadPubCompProperties(subDecodeError: DecodeError) extends DecodeError

  /**
   * Something is wrong with the subscribe message
   */
  final case class BadSubscribeMessage(subDecodeError: DecodeError) extends DecodeError
  final case class BadTopicFilter(subDecodeError: DecodeError) extends DecodeError
  case object EmptyTopicFilterListNotAllowed extends DecodeError
  final case class BadSubscribeProperties(subDecodeError: DecodeError) extends DecodeError
  final case class BadSubscriptionOption(subDecodeError: DecodeError) extends DecodeError
  case object SubscriptionOptionReservedBitSet extends DecodeError

  /**
   * Something is wrong with the subscribe acknowledge message
   */
  final case class BadSubAckMessage(subDecodeError: DecodeError) extends DecodeError
  final case class BadSubAckProperties(subDecodeError: DecodeError) extends DecodeError

  /**
   * Something is wrong with the unsubscribe message
   */
  final case class BadUnsubscribeMessage(subDecodeError: DecodeError) extends DecodeError
  final case class BadUnsubscribeProperties(subDecodeError: DecodeError) extends DecodeError

  /**
   * Something is wrong with the unsubscribe acknowledge message
   */
  final case class BadUnsubAckMessage(subDecodeError: DecodeError) extends DecodeError
  final case class BadUnsubAckProperties(subDecodeError: DecodeError) extends DecodeError

  /**
   * Something is wrong with the disconnect message
   */
  final case class BadDisconnectMessage(subDecodeError: DecodeError) extends DecodeError
  final case class BadDisconnectProperties(subDecodeError: DecodeError) extends DecodeError

  /**
   * Something is wrong with the authentication exchange message
   */
  final case class BadAuthMessage(subDecodeError: DecodeError) extends DecodeError
  final case class BadAuthProperties(subDecodeError: DecodeError) extends DecodeError

  /**
   * Property which must be defined only one time occured multiple times.
   */
  final case object SessionExpiryIntervalDefinedMoreThanOnce extends ProtocolError
  final case object ReceiveMaximumDefinedMoreThanOnce extends ProtocolError
  final case object MaximumPacketSizeDefinedMoreThanOnce extends ProtocolError
  final case object TopicAliasDefinedMoreThanOnce extends ProtocolError
  final case object RequestResponseInformationDefinedMoreThanOnce extends ProtocolError
  final case object RequestProblemInformationDefinedMoreThanOnce extends ProtocolError
  final case object AuthenticationMethodDefinedMoreThanOnce extends ProtocolError
  final case object AuthenticationDataDefinedMoreThanOnce extends ProtocolError
  final case object WillDelayIntervalDefinedMoreThanOnce extends ProtocolError
  final case object PayloadFormatIndicatorDefinedMoreThanOnce extends ProtocolError
  final case object MessageExpiryIntervalDefinedMoreThanOnce extends ProtocolError
  final case object ContentTypeDefinedMoreThanOnce extends ProtocolError
  final case object ResponseTopicDefinedMoreThanOnce extends ProtocolError
  final case object CorrelationDataDefinedMoreThanOnce extends ProtocolError
  final case object SubscriptionIdentifierDefinedMoreThanOnce extends ProtocolError
  final case object CotentTypeDefinedMoreThanOnce extends ProtocolError
  final case object MaximumQosDefinedMoreThanOnce extends ProtocolError
  final case object RetainAvailableDefinedMoreThanOnce extends ProtocolError
  final case object AssignedClientIdentifierDefinedMoreThanOnce extends ProtocolError
  final case object TopicAliasMaximumDefinedMoreThanOnce extends ProtocolError
  final case object WildcardSubscriptionAvailableDefinedMoreThanOnce extends ProtocolError
  final case object SubscriptionIdentifierAvailableDefinedMoreThanOnce extends ProtocolError
  final case object SharedSubscriptionAvailableDefinedMoreThanOnce extends ProtocolError
  final case object ServerKeepAliveDefinedMoreThanOnce extends ProtocolError
  final case object ResponseInformationDefinedMoreThanOnce extends ProtocolError
  final case object ServerReferenceDefinedMoreThanOnce extends ProtocolError
  final case object ReasonStringDefinedMoreThanOnce extends ProtocolError


  /**
   * Unvalid property value
   */
  final case class UnvalidRequestResponseInformationValue(v: Int) extends ProtocolError
  final case class UnvalidRequestProblemInformationValue(v: Int) extends ProtocolError
  final case class UnvalidMaximumQosValue(v: Int) extends ProtocolError
  final case class UnvalidRetainAvailableValue(v: Int) extends ProtocolError
  final case class UnvalidWildcardSubscriptionAvailableValue(v: Int) extends ProtocolError
  final case class UnvalidSubscriptionIdentifierAvailableValue(v: Int) extends ProtocolError
  final case class UnvalidSharedSubscriptionAvailableValue(v: Int) extends ProtocolError
  final case class UnvalidPayloadFormatIndicatorValue(v: Int) extends PayloadFormatInvalid

  /**
   * Authentication Method undefined
   */
  final case object AuthMethodUndefined extends ProtocolError

  /**
   * Authentication Data defined when no Authentication Method is set
   */
  final case object AuthDataDefinedWhileAuthMethodUndefined extends ProtocolError

  /**
   * Bits 1 to 7 are set to zero in the Connect Ack flags
   */
  case object ConnectAckFlagReservedBitsSet extends DecodeError

  // 1.5.4 UTF-8 encoded strings
  implicit class MqttString(val v: String) extends AnyVal {
    def encode(bsb: ByteStringBuilder): ByteStringBuilder = {
      val length = v.length & 0xffff
      bsb.putShort(length).putBytes(v.getBytes(StandardCharsets.UTF_8), 0, length)
    }
  }

  // 1.5.5 Variable Byte Integer
  implicit class MqttVariableByteInteger(val v: VariableByteInteger) {
    def encode(bsb: ByteStringBuilder): ByteStringBuilder = {
      val v0 = v.underlying
      val b0 = v0
      val b1 = v0 >> 7
      val b2 = v0 >> 14
      val b3 = v0 >> 21
      bsb.putByte(((b0 & 0x7f).toByte | (if (b1 > 0) 0x80 else 0x00)).toByte)
      if (b1 > 0) bsb.putByte(((b1 & 0x7f) | (if (b2 > 0) 0x80 else 0x00)).toByte)
      if (b2 > 0) bsb.putByte(((b2 & 0x7f) | (if (b3 > 0) 0x80 else 0x00)).toByte)
      if (b3 > 0) bsb.putByte(b3.toByte)
      bsb
    }
  }

  // 1.5.6 BinaryData
  implicit class MqttBinaryData(val v: BinaryData) {
    def encode(bsb: ByteStringBuilder): ByteStringBuilder = {
      val length = v.underlying.length
      bsb.putShort(length)
      v.underlying.foreach(b => bsb.putByte(b))
      bsb
    }
  }

  // 1.5.7 UTF-8 String Pair
  implicit class MqttStringPair(val v: (String, String)) {
    def encode(bsb: ByteStringBuilder): ByteStringBuilder = {
      v match {
        case (name, value) =>
          name.encode(bsb)
          value.encode(bsb)
      }
      bsb
    }
  }

  // 2.2.2.2 Property - User Property
  implicit class MqttUserProperties(val v: List[(String, String)]) {
    def encode(bsb: ByteStringBuilder): ByteStringBuilder = {
      v.foreach(pair => {
        PropertyIdentifiers.UserProperty.encode(bsb)
        pair.encode(bsb)
      })
      bsb
    }
  }

  // 2 MQTT Control Packet format
  implicit class MqttControlPacket(val v: ControlPacket) extends AnyVal {

    def encode(bsb: ByteStringBuilder, remainingLength: Int): ByteStringBuilder = {
      bsb.putByte((v.packetType.underlying << 4 | v.flags.underlying).toByte)
      VariableByteInteger(remainingLength).encode(bsb)
    }
  }

  // 3.1 CONNECT – Client requests a connection to a Server
  implicit class MqttConnect(val v: Connect) extends AnyVal {
    def encode(bsb: ByteStringBuilder, protocolLevel: Connect.ProtocolLevel): ByteStringBuilder = {
      val variableHeaderBsb = ByteString.newBuilder
      // Variable header
      Connect.Mqtt.encode(variableHeaderBsb)
      variableHeaderBsb.putByte(protocolLevel.toByte)
      variableHeaderBsb.putByte(v.connectFlags.underlying.toByte)
      variableHeaderBsb.putShort(v.keepAlive.toSeconds.toShort)
      // Properties
      val propsBsb = ByteString.newBuilder
      if (protocolLevel == Connect.v5)
        v.properties.encode(propsBsb, protocolLevel)
      else if (v.properties != ConnectProperties())
        protocolLevelInsufficient(protocolLevel, Connect.v5, "MQTT Protocol Level 5 is necessary in order to encode Connect Properties")
      // Payload
      val payloadBsb = ByteString.newBuilder
      v.payload.ClientIdentifier.encode(payloadBsb)
      v.payload.WillProperties.foreach { willProperties =>
        if (protocolLevel == Connect.v5)
          willProperties.encode(payloadBsb, protocolLevel)
        else if (willProperties != ConnectPayloadWillProperties())
          protocolLevelInsufficient(protocolLevel, Connect.v5, "MQTT Protocol Level 5 is necessary in order to encode Connect Payload Will Properties")
      }
      v.payload.WillTopic.foreach(_.encode(payloadBsb))
      v.payload.WillPayload.foreach(_.encode(payloadBsb))
      v.payload.UserName.foreach(_.encode(payloadBsb))
      v.payload.Password.foreach(_.encode(payloadBsb))
      val packetBsb = variableHeaderBsb
        .append(propsBsb.result())
        .append(payloadBsb.result())
      // Fixed header
      (v: ControlPacket).encode(bsb, packetBsb.length)
      bsb.append(packetBsb.result())
    }
  }

  // 3.1.2.11 CONNECT Properties
  implicit class MqttConnectProperties(val v: ConnectProperties) extends AnyVal {
    def encode(bsb: ByteStringBuilder, protocolLevel: Connect.ProtocolLevel): ByteStringBuilder = {
      val propertiesBsb = ByteString.newBuilder
      v.SessionExpiryInterval.foreach(v0 => {
        PropertyIdentifiers.SessionExpiryInterval.encode(propertiesBsb)
        propertiesBsb.putInt(v0)
      })
      v.ReceiveMaximum.foreach(v0 => {
        PropertyIdentifiers.ReceiveMaximum.encode(propertiesBsb)
        propertiesBsb.putShort(v0)
      })
      v.MaximumPacketSize.foreach(v0 => {
        PropertyIdentifiers.MaximumPacketSize.encode(propertiesBsb)
        propertiesBsb.putInt(v0)
      })
      v.TopicAliasMaximum.foreach(v0 => {
        PropertyIdentifiers.TopicAliasMaximum.encode(propertiesBsb)
        propertiesBsb.putShort(v0)
      })
      v.RequestResponseInformation.foreach(v0 => {
        PropertyIdentifiers.RequestResponseInformation.encode(propertiesBsb)
        propertiesBsb.putByte(v0.toByte)
      })
      v.RequestProblemInformation.foreach(v0 => {
        PropertyIdentifiers.RequestProblemInformation.encode(propertiesBsb)
        propertiesBsb.putByte(v0.toByte)
      })
      v.UserProperties.foreach(_.encode(propertiesBsb))
      v.AuthenticationMethod.foreach(v0 => {
        PropertyIdentifiers.AuthenticationMethod.encode(propertiesBsb)
        v0.encode(propertiesBsb)
      })
      v.AuthenticationData.foreach(v0 => {
        PropertyIdentifiers.AuthenticationData.encode(propertiesBsb)
        v0.encode(propertiesBsb)
      })
      VariableByteInteger(propertiesBsb.length).encode(bsb)
      bsb.append(propertiesBsb.result())
    }
  }

  // 3.1.3 CONNECT Payload
  implicit class MqttPayload(val v: ConnectPayload) extends AnyVal {
    def encode(bsb: ByteStringBuilder, protocolLevel: Connect.ProtocolLevel): ByteStringBuilder = {
      val payloadBsb = ByteString.newBuilder
      v.ClientIdentifier.encode(payloadBsb)
      v.WillProperties.foreach(_.encode(payloadBsb, protocolLevel))
      v.WillTopic.foreach(_.encode(payloadBsb))
      v.WillPayload.foreach(_.encode(payloadBsb))
      v.UserName.foreach(_.encode(payloadBsb))
      v.Password.foreach(_.encode(payloadBsb))
      bsb
    }
  }

  // 3.1.3.2 CONNECT Payload Will Properties
  implicit class MqttPayloadConnectWillProperties(val v: ConnectPayloadWillProperties) extends AnyVal {
    def encode(bsb: ByteStringBuilder, protocolLevel: Connect.ProtocolLevel): ByteStringBuilder = {
      val propertiesBsb = ByteString.newBuilder
      v.WillDelayInterval.foreach(v0 => {
        PropertyIdentifiers.WillDelayInterval.encode(propertiesBsb)
        propertiesBsb.putInt(v0)
      })
      v.PayloadFormatIndicator.foreach(v0 => {
        PropertyIdentifiers.PayloadFormatIndicator.encode(propertiesBsb)
        propertiesBsb.putByte(v0.toByte)
      })
      v.MessageExpiryInterval.foreach(v0 => {
        PropertyIdentifiers.MessageExpiryInterval.encode(propertiesBsb)
        propertiesBsb.putInt(v0)
      })
      v.ContentType.foreach(v0 => {
        PropertyIdentifiers.ContentType.encode(propertiesBsb)
        v0.encode(propertiesBsb)
      })
      v.ResponseTopic.foreach(v0 => {
        PropertyIdentifiers.ResponseTopic.encode(propertiesBsb)
        v0.encode(propertiesBsb)
      })
      v.CorrelationData.foreach(v0 => {
        PropertyIdentifiers.CorrelationData.encode(propertiesBsb)
        v0.encode(propertiesBsb)
      })
      v.UserProperties.foreach(_.encode(propertiesBsb))
      VariableByteInteger(propertiesBsb.length).encode(bsb)
      bsb.append(propertiesBsb.result())
    }
  }

  // 3.2 CONNACK – Acknowledge connection request
  implicit class MqttConnAck(val v: ConnAck) extends AnyVal {
    def encode(bsb: ByteStringBuilder, protocolLevel: Connect.ProtocolLevel): ByteStringBuilder = {
      val variableHeaderBsb = ByteString.newBuilder
      // Variable header
      variableHeaderBsb.putByte(v.connAckFlags.underlying.toByte)
      variableHeaderBsb.putByte(v.reasonCode.underlying.toByte)
      // Properties
      val propsBsb = ByteString.newBuilder
      if (protocolLevel == Connect.v5)
        v.properties.encode(propsBsb)
      else if (v.properties != ConnAckProperties())
        protocolLevelInsufficient(protocolLevel, Connect.v5, "MQTT Protocol Level 5 is necessary in order to encode ConnAck Properties")
      variableHeaderBsb.append(propsBsb.result())
      // Fixed header
      (v: ControlPacket).encode(bsb, variableHeaderBsb.length)
      bsb.append(variableHeaderBsb.result())
    }
  }

  // 3.2.2.3 CONNACK Properties
  implicit class MqttConnAckProperties(val v: ConnAckProperties) extends AnyVal {
    def encode(bsb: ByteStringBuilder): ByteStringBuilder = {
      val propertiesBsb = ByteString.newBuilder
      v.SessionExpiryInterval.foreach(v0 => {
        PropertyIdentifiers.SessionExpiryInterval.encode(propertiesBsb)
        propertiesBsb.putInt(v0)
      })
      v.ReceiveMaximum.foreach(v0 => {
        PropertyIdentifiers.ReceiveMaximum.encode(propertiesBsb)
        propertiesBsb.putShort(v0)
      })
      v.MaximumQoS.foreach(v0 => {
        PropertyIdentifiers.MaximumQoS.encode(propertiesBsb)
        propertiesBsb.putByte(v0.toByte)
      })
      v.RetainAvailable.foreach(v0 => {
        PropertyIdentifiers.RetainAvailable.encode(propertiesBsb)
        propertiesBsb.putByte(v0.toByte)
      })
      v.MaximumPacketSize.foreach(v0 => {
        PropertyIdentifiers.MaximumPacketSize.encode(propertiesBsb)
        propertiesBsb.putInt(v0)
      })
      v.AssignedClientIdentifier.foreach(v0 => {
        PropertyIdentifiers.AssignedClientIdentifier.encode(propertiesBsb)
        v0.encode(propertiesBsb)
      })
      v.TopicAliasMaximum.foreach(v0 => {
        PropertyIdentifiers.TopicAliasMaximum.encode(propertiesBsb)
        propertiesBsb.putShort(v0)
      })
      v.ReasonString.foreach(v0 => {
        PropertyIdentifiers.ReasonString.encode(propertiesBsb)
        v0.encode(propertiesBsb)
      })
      v.UserProperties.foreach(_.encode(propertiesBsb))
      v.WildcardSubscriptionAvailable.foreach(v0 => {
        PropertyIdentifiers.WildcardSubscriptionAvailable.encode(propertiesBsb)
        propertiesBsb.putByte(v0.toByte)
      })
      v.SubscriptionIdentifierAvailable.foreach(v0 => {
        PropertyIdentifiers.SubscriptionIdentifierAvailable.encode(propertiesBsb)
        propertiesBsb.putByte(v0.toByte)
      })
      v.SharedSubscriptionAvailable.foreach(v0 => {
        PropertyIdentifiers.SharedSubscriptionAvailable.encode(propertiesBsb)
        propertiesBsb.putByte(v0.toByte)
      })
      v.ServerKeepAlive.foreach(v0 => {
        PropertyIdentifiers.ServerKeepAlive.encode(propertiesBsb)
        propertiesBsb.putShort(v0)
      })
      v.ResponseInformation.foreach(v0 => {
        PropertyIdentifiers.ResponseInformation.encode(propertiesBsb)
        v0.encode(propertiesBsb)
      })
      v.ServerReference.foreach(v0 => {
        PropertyIdentifiers.ServerReference.encode(propertiesBsb)
        v0.encode(propertiesBsb)
      })
      v.AuthenticationMethod.foreach(v0 => {
        PropertyIdentifiers.AuthenticationMethod.encode(propertiesBsb)
        v0.encode(propertiesBsb)
      })
      v.AuthenticationData.foreach(v0 => {
        PropertyIdentifiers.AuthenticationData.encode(propertiesBsb)
        v0.encode(propertiesBsb)
      })
      VariableByteInteger(propertiesBsb.length).encode(bsb)
      bsb.append(propertiesBsb.result())
    }
  }

  // 3.3 PUBLISH – Publish message
  implicit class MqttPublish(val v: Publish) extends AnyVal {
    def encode(bsb: ByteStringBuilder, packetId: Option[PacketId], protocolLevel: Connect.ProtocolLevel): ByteStringBuilder = {
      // Variable header
      val variableHeaderBsb = ByteString.newBuilder
      v.topicName.encode(variableHeaderBsb)
      packetId.foreach(pId=> variableHeaderBsb.putShort(pId.underlying.toShort))
      // Properties
      val propsBsb = ByteString.newBuilder
      if (protocolLevel == Connect.v5)
        v.properties.encode(propsBsb)
      else if (v.properties != PublishProperties())
        protocolLevelInsufficient(protocolLevel, Connect.v5, "MQTT Protocol Level 5 is necessary in order to encode Publish Properties")
      // Payload
      val payloadBsb = ByteString.newBuilder.append(v.payload)
      val packetBsb = variableHeaderBsb
        .append(propsBsb.result())
        .append(payloadBsb.result())
      // Fixed header
      (v: ControlPacket).encode(bsb, packetBsb.length)

      bsb.append(packetBsb.result())
    }
  }

  // 3.3.2.3 PUBLISH Properties
  implicit class MqttPublishProperties(val v: PublishProperties) extends AnyVal {
    def encode(bsb: ByteStringBuilder): ByteStringBuilder = {
      val propertiesBsb = ByteString.newBuilder
      v.PayloadFormatIndicator.foreach(v0 => {
        PropertyIdentifiers.PayloadFormatIndicator.encode(propertiesBsb)
        propertiesBsb.putByte(v0.toByte)
      })
      v.MessageExpiryInterval.foreach(v0 => {
        PropertyIdentifiers.MessageExpiryInterval.encode(propertiesBsb)
        propertiesBsb.putInt(v0)
      })
      v.TopicAlias.foreach(v0 => {
        PropertyIdentifiers.TopicAlias.encode(propertiesBsb)
        propertiesBsb.putShort(v0)
      })
      v.ResponseTopic.foreach(v0 => {
        PropertyIdentifiers.ResponseTopic.encode(propertiesBsb)
        v0.encode(propertiesBsb)
      })
      v.CorrelationData.foreach(v0 => {
        PropertyIdentifiers.CorrelationData.encode(propertiesBsb)
        v0.encode(propertiesBsb)
      })
      v.UserProperties.foreach(_.encode(propertiesBsb))
      v.SubscriptionIdentifier.foreach(v0 => {
        PropertyIdentifiers.SubscriptionIdentifier.encode(propertiesBsb)
        VariableByteInteger(v0).encode(propertiesBsb)
      })
      v.ContentType.foreach(v0 => {
        PropertyIdentifiers.ContentType.encode(propertiesBsb)
        v0.encode(propertiesBsb)
      })
      VariableByteInteger(propertiesBsb.length).encode(bsb)
      bsb.append(propertiesBsb.result())
    }
  }

  // 3.4 PUBACK – Publish acknowledgement
  implicit class MqttPubAck(val v: PubAck) extends AnyVal {
    def encode(bsb: ByteStringBuilder, protocolLevel: Connect.ProtocolLevel): ByteStringBuilder = {
      val variableHeaderBsb = ByteString.newBuilder
      variableHeaderBsb.putShort(v.packetId.underlying)
      if (protocolLevel == Connect.v5) {
        variableHeaderBsb.putByte(v.reasonCode.underlying.toByte)
        v.properties.foreach(_.encode(variableHeaderBsb))
      }
      else
        v.properties.foreach { props =>
          if (props != PubAckProperties())
            protocolLevelInsufficient(protocolLevel, Connect.v5, "MQTT Protocol Level 5 is necessary in order to encode Publish Acknowledge Properties")
        }
      (v: ControlPacket).encode(bsb, variableHeaderBsb.length)
      bsb.append(variableHeaderBsb.result())
      bsb
    }
  }

  // 3.4.1.1 PUBACK Properties
  implicit class MqttPubAckProperties(val v: PubAckProperties) {
    def encode(bsb: ByteStringBuilder): ByteStringBuilder = {
      val propertiesBsb = ByteString.newBuilder
      v.ReasonString.foreach(v0 => {
        PropertyIdentifiers.ReasonString.encode(propertiesBsb)
        v0.encode(propertiesBsb)
      })
      v.UserProperties.foreach(_.encode(propertiesBsb))
      VariableByteInteger(propertiesBsb.length).encode(bsb)
      bsb.append(propertiesBsb.result())
    }
  }

  // 3.5 PUBREC – Publish received (QoS 2 publish received, part 1)
  implicit class MqttPubRec(val v: PubRec) extends AnyVal {
    def encode(bsb: ByteStringBuilder, protocolLevel: Connect.ProtocolLevel): ByteStringBuilder = {
      val variableHeaderBsb = ByteString.newBuilder
      variableHeaderBsb.putShort(v.packetId.underlying)
      v.properties match {
        case None =>
          if (v.reasonCode.underlying != 0)
            variableHeaderBsb.putByte(v.reasonCode.underlying.toByte)
        case Some(props) =>
          variableHeaderBsb.putByte(v.reasonCode.underlying.toByte)
          props.encode(variableHeaderBsb, protocolLevel: Connect.ProtocolLevel)
      }
      (v: ControlPacket).encode(bsb, variableHeaderBsb.length)
      bsb.append(variableHeaderBsb.result())
      bsb
    }
  }

  // 3.5.2.2 PUBREC Properties
  implicit class MqttPubRecProperties(val v: PubRecProperties) {
    def encode(bsb: ByteStringBuilder, protocolLevel: Connect.ProtocolLevel): ByteStringBuilder = {
      val propertiesBsb = ByteString.newBuilder
      v.ReasonString.foreach(v0 => {
        PropertyIdentifiers.ReasonString.encode(propertiesBsb)
        v0.encode(propertiesBsb)
      })
      v.UserProperties.foreach(_.encode(propertiesBsb))
      VariableByteInteger(propertiesBsb.length).encode(bsb)
      bsb.append(propertiesBsb.result())
    }
  }

  // 3.6 PUBREL – Publish release (QoS 2 delivery part 1)
  implicit class MqttPubRel(val v: PubRel) extends AnyVal {
    def encode(bsb: ByteStringBuilder, protocolLevel: Connect.ProtocolLevel): ByteStringBuilder = {
      val variableHeaderBsb = ByteString.newBuilder
      variableHeaderBsb.putShort(v.packetId.underlying)
      v.properties match {
        case None =>
          if (v.reasonCode.underlying != 0)
            variableHeaderBsb.putByte(v.reasonCode.underlying.toByte)
        case Some(props) =>
          variableHeaderBsb.putByte(v.reasonCode.underlying.toByte)
          props.encode(variableHeaderBsb, protocolLevel: Connect.ProtocolLevel)
      }
      (v: ControlPacket).encode(bsb, variableHeaderBsb.length)
      bsb.append(variableHeaderBsb.result())
      bsb
    }
  }

  // 3.6.2.2 PUBREL Properties
  implicit class MqttPubRelProperties(val v: PubRelProperties) {
    def encode(bsb: ByteStringBuilder, protocolLevel: Connect.ProtocolLevel): ByteStringBuilder = {
      val propertiesBsb = ByteString.newBuilder
      v.ReasonString.foreach(v0 => {
        PropertyIdentifiers.ReasonString.encode(propertiesBsb)
        v0.encode(propertiesBsb)
      })
      v.UserProperties.foreach(_.encode(propertiesBsb))
      VariableByteInteger(propertiesBsb.length).encode(bsb)
      bsb.append(propertiesBsb.result())
    }
  }

  // 3.7 PUBCOMP – Publish comlete (QoS 2 delivery part 3)
  implicit class MqttPubComp(val v: PubComp) extends AnyVal {
    def encode(bsb: ByteStringBuilder, protocolLevel: Connect.ProtocolLevel): ByteStringBuilder = {
      val variableHeaderBsb = ByteString.newBuilder
      variableHeaderBsb.putShort(v.packetId.underlying)
      v.properties match {
        case None =>
          if (v.reasonCode.underlying != 0)
            variableHeaderBsb.putByte(v.reasonCode.underlying.toByte)
        case Some(props) =>
          variableHeaderBsb.putByte(v.reasonCode.underlying.toByte)
          props.encode(variableHeaderBsb, protocolLevel: Connect.ProtocolLevel)
      }
      (v: ControlPacket).encode(bsb, variableHeaderBsb.length)
      bsb.append(variableHeaderBsb.result())
      bsb
    }
  }

  // 3.7.2.2 PUBCOMP Properties
  implicit class MqttPubCompProperties(val v: PubCompProperties) {
    def encode(bsb: ByteStringBuilder, protocolLevel: Connect.ProtocolLevel): ByteStringBuilder = {
      val propertiesBsb = ByteString.newBuilder
      v.ReasonString.foreach(v0 => {
        PropertyIdentifiers.ReasonString.encode(propertiesBsb)
        v0.encode(propertiesBsb)
      })
      v.UserProperties.foreach(_.encode(propertiesBsb))
      VariableByteInteger(propertiesBsb.length).encode(bsb)
      bsb.append(propertiesBsb.result())
    }
  }

  // 3.8 SUBSCRIBE - Subscribe to topics
  implicit class MqttSubscribe(val v: Subscribe) extends AnyVal {
    def encode(bsb: ByteStringBuilder, packetId: PacketId, protocolLevel: Connect.ProtocolLevel) : ByteStringBuilder = {
      val variableHeaderBsb = ByteString.newBuilder
      // Variable header
      variableHeaderBsb.putShort(packetId.underlying.toShort)
      // Properties
      if (protocolLevel == Connect.v5)
        v.properties.encode(variableHeaderBsb)
      else if (v.properties != SubscribeProperties())
        protocolLevelInsufficient(protocolLevel, Connect.v5, "MQTT Protocol Level 5 is necessary in order to encode Subscribe Properties")
      // Payload
      v.topicFilters.foreach {
        case (topicFilter, topicFilterFlags) =>
          topicFilter.encode(variableHeaderBsb)
          val flags = topicFilterFlags.underlying.toByte
          if (protocolLevel < Connect.v5) {
            // Check 'Retain Handling' flag
            if ((flags & 0x10) > 0 || (flags & 0x20) > 0)
              protocolLevelInsufficient(protocolLevel, Connect.v5, "MQTT Protocol Level 5 is necessary in order to use Topic Filter Flag 'Retain Handling'")
            // Check 'Retain as Published' flag
            if ((flags & 0x08) > 0)
              protocolLevelInsufficient(protocolLevel, Connect.v5, "MQTT Protocol Level 5 is necessary in order to use Topic Filter Flag 'Retain as Published (RAP)'")
            // Check 'No Local' flag
            if ((flags & 0x04) > 0)
              protocolLevelInsufficient(protocolLevel, Connect.v5, "MQTT Protocol Level 5 is necessary in order to use Topic Filter Flag 'No Local (NL)'")
          }
          variableHeaderBsb.putByte(topicFilterFlags.underlying.toByte)
      }
      // Fixed header
      (v: ControlPacket).encode(bsb, variableHeaderBsb.length)
      bsb.append(variableHeaderBsb.result())
    }
  }

  // 3.8.2.1 SUBSCRIBE Properties
  implicit class MqttSubscribeProperties(val v: SubscribeProperties) extends AnyVal {
    def encode(bsb: ByteStringBuilder): ByteStringBuilder = {
      val propertiesBsb = ByteString.newBuilder
      v.SubscriptionIdentifier.foreach(v0 => {
        PropertyIdentifiers.SubscriptionIdentifier.encode(propertiesBsb)
        VariableByteInteger(v0).encode(propertiesBsb)
      })
      v.UserProperties.foreach(_.encode(propertiesBsb))
      VariableByteInteger(propertiesBsb.length).encode(bsb)
      bsb.append(propertiesBsb.result())
    }
  }

  // 3.9 SUBACK – Subscribe acknowledgement
  implicit class MqttSubAck(val v: SubAck) extends AnyVal {
    def encode(bsb: ByteStringBuilder, protocolLevel: Connect.ProtocolLevel): ByteStringBuilder = {
      val variableHeaderBsb = ByteString.newBuilder
      // Variable header
      variableHeaderBsb.putShort(v.packetId.underlying.toShort)
      // Properties
      if (protocolLevel == Connect.v5)
        v.properties.encode(variableHeaderBsb)
      else if (v.properties != SubAckProperties())
        protocolLevelInsufficient(protocolLevel, Connect.v5, "MQTT Protocol Level 5 is necessary in order to encode Subscribe Acknowledge Properties")
      // Payload
      v.reasonCodes.foreach { returnCode =>
        if (protocolLevel < Connect.v5) {
          if ((returnCode != SubAckReasonCode.GrantedQoS0)
            && (returnCode != SubAckReasonCode.GrantedQoS1)
            && (returnCode != SubAckReasonCode.GrantedQoS2)
            && (returnCode != SubAckReasonCode.UnspecifiedError))
            protocolLevelInsufficient(protocolLevel, Connect.v5, "MQTT Protocol Level 5 is necessary in order to encode Subscribe Acknowledge Reason Codes unequal to 0,1,2 or 128")
        }
        variableHeaderBsb.putByte(returnCode.underlying.toByte)
      }
      // Fixed header
      (v: ControlPacket).encode(bsb, variableHeaderBsb.length)
      bsb.append(variableHeaderBsb.result())
    }
  }

  // 3.9.2.1 SUBACK Properties
  implicit class MqttSubAckProperties(val v: SubAckProperties) extends AnyVal {
    def encode(bsb: ByteStringBuilder): ByteStringBuilder = {
      val propertiesBsb = ByteString.newBuilder
      v.ReasonString.foreach(v0 => {
        PropertyIdentifiers.ReasonString.encode(propertiesBsb)
        v0.encode(propertiesBsb)
      })
      v.UserProperties.foreach(_.encode(propertiesBsb))
      VariableByteInteger(propertiesBsb.length).encode(bsb)
      bsb.append(propertiesBsb.result())
    }
  }

  // 3.10 UNSUBSCRIBE – Unsubscribe from topics
  implicit class MqttUnsubscribe(val v: Unsubscribe) extends AnyVal {
    def encode(bsb: ByteStringBuilder, packetId: PacketId, protocolLevel: Connect.ProtocolLevel): ByteStringBuilder = {
      val variableHeaderBsb = ByteString.newBuilder
      // Variable header
      variableHeaderBsb.putShort(packetId.underlying.toShort)
      // Properties
      v.properties.encode(variableHeaderBsb, protocolLevel)
      // Payload
      v.topicFilters.foreach(_.encode(variableHeaderBsb))
      // Fixed header
      (v: ControlPacket).encode(bsb, variableHeaderBsb.length)
      bsb.append(variableHeaderBsb.result())
    }
  }

  // 3.10.2.1 UNSUBSCRIBE Properties
  implicit class MqttUnsubscribeProperties(val v: UnsubscribeProperties) extends AnyVal {
    def encode(bsb: ByteStringBuilder, protocolLevel: Connect.ProtocolLevel): ByteStringBuilder = {
      val propertiesBsb = ByteString.newBuilder
      v.UserProperties.foreach(_.encode(propertiesBsb))
      VariableByteInteger(propertiesBsb.length).encode(bsb)
      bsb.append(propertiesBsb.result())
    }
  }

  // 3.11 UNSUBACK – Unsubscribe acknowledgement
  implicit class MqttUnsubAck(val v: UnsubAck) extends AnyVal {
    def encode(bsb: ByteStringBuilder, protocolLevel: Connect.ProtocolLevel): ByteStringBuilder = {
      val variableHeaderBsb = ByteString.newBuilder
      // Variable header
      variableHeaderBsb.putShort(v.packetId.underlying.toShort)
      // Properties
      v.properties.encode(variableHeaderBsb, protocolLevel)
      // Payload
      v.reasonCodes.foreach { returnCode =>
        variableHeaderBsb.putByte(returnCode.underlying.toByte)
      }
      // Fixed header
      (v: ControlPacket).encode(bsb, variableHeaderBsb.length)
      bsb.append(variableHeaderBsb.result())
    }
  }

  // 3.11.2.1 UNSUBACK Properties
  implicit class MqttUnsubAckProperties(val v: UnsubAckProperties) extends AnyVal {
    def encode(bsb: ByteStringBuilder, protocolLevel: Connect.ProtocolLevel): ByteStringBuilder = {
      val propertiesBsb = ByteString.newBuilder
      v.ReasonString.foreach(v0 => {
        PropertyIdentifiers.ReasonString.encode(propertiesBsb)
        v0.encode(propertiesBsb)
      })
      v.UserProperties.foreach(_.encode(propertiesBsb))
      VariableByteInteger(propertiesBsb.length).encode(bsb)
      bsb.append(propertiesBsb.result())
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
  implicit class MqttDisconnect(val v: Disconnect) extends AnyVal {
    def encode(bsb: ByteStringBuilder, protocolLevel: Connect.ProtocolLevel): ByteStringBuilder = {
      val variableHeaderBsb = ByteString.newBuilder
      if ((v.reasonCode != DisconnectReasonCode.NormalDisconnection)
        || (v.properties != DisconnectProperties())) {
        if (protocolLevel < Connect.v5)
          protocolLevelInsufficient(protocolLevel, Connect.v5, "MQTT Protocol Level 5 is necessary in order to encode Disconnect Reason Codes and/or Properties")
        // Reason Code
        variableHeaderBsb.putByte(v.reasonCode.underlying.toByte)
        // Properties
        v.properties.encode(variableHeaderBsb)
      }
      (v: ControlPacket).encode(bsb, variableHeaderBsb.length)
      bsb.append(variableHeaderBsb.result())
    }
  }

  // 3.14.2.2 DISCONNECT Properties
  implicit class MqttDisconnectProperties(val v: DisconnectProperties) extends AnyVal {
    def encode(bsb: ByteStringBuilder): ByteStringBuilder = {
      val propertiesBsb = ByteString.newBuilder
      v.SessionExpiryInterval.foreach(v0 => {
        PropertyIdentifiers.SessionExpiryInterval.encode(propertiesBsb)
        propertiesBsb.putInt(v0)
      })
      v.ReasonString.foreach(v0 => {
        PropertyIdentifiers.ReasonString.encode(propertiesBsb)
        v0.encode(propertiesBsb)
      })
      v.UserProperties.foreach(_.encode(propertiesBsb))
      v.ServerReference.foreach(v0 => {
        PropertyIdentifiers.ServerReference.encode(propertiesBsb)
        v0.encode(propertiesBsb)
      })
      VariableByteInteger(propertiesBsb.length).encode(bsb)
      bsb.append(propertiesBsb.result())
    }
  }

  // 3.15 AUTH - Authentication Exchange
  implicit class MqttAuth(val v: Auth) extends AnyVal {
    def encode(bsb: ByteStringBuilder, protocolLevel: Connect.ProtocolLevel): ByteStringBuilder = {
      if (protocolLevel < Connect.v5)
        protocolLevelInsufficient(protocolLevel, Connect.v5, "MQTT Protocol Level 5 is necessary in order to encode Auth Packet")

      val variableHeaderBsb = ByteString.newBuilder
      // Reason Code
      variableHeaderBsb.putByte(v.reasonCode.underlying.toByte)
      // Properties
      v.properties.encode(variableHeaderBsb)
      (v: ControlPacket).encode(bsb, variableHeaderBsb.length)
      bsb.append(variableHeaderBsb.result())
    }
  }

  // 3.15.2.2 AUTH Properties
  implicit class MqttAuthProperties(val v: AuthProperties) extends AnyVal {
    def encode(bsb: ByteStringBuilder): ByteStringBuilder = {
      val propertiesBsb = ByteString.newBuilder
      PropertyIdentifiers.AuthenticationMethod.encode(propertiesBsb)
      v.AuthenticationMethod.encode(propertiesBsb)
      v.AuthenticationData.foreach(v0 => {
        PropertyIdentifiers.AuthenticationData.encode(propertiesBsb)
        v0.encode(propertiesBsb)
      })
      v.ReasonString.foreach(v0 => {
        PropertyIdentifiers.ReasonString.encode(propertiesBsb)
        v0.encode(propertiesBsb)
      })
      v.UserProperties.foreach(_.encode(propertiesBsb))
      VariableByteInteger(propertiesBsb.length).encode(bsb)
      bsb.append(propertiesBsb.result())
    }
  }

  implicit class MqttByteIterator(val v: ByteIterator) extends AnyVal {

    // 1.5.2 Two Byte Integer
    def decodeTwoByteInteger(): Either[DecodeError, Int] =
      try {
        Right(v.getShort & 0xffff)
      } catch {
        case _: NoSuchElementException => Left(BufferUnderflow)
      }

    // 1.5.3 Two Byte Integer
    def decodeFourByteInteger(): Either[DecodeError, Int] =
      try {
        Right(v.getInt & 0xffffffff)
      } catch {
        case _: NoSuchElementException => Left(BufferUnderflow)
      }

    // 1.5.4 UTF-8 encoded strings
    def decodeString(): Either[DecodeError, String] =
      try {
        val length = v.getShort & 0xffff
        Right(v.getByteString(length).utf8String)
      } catch {
        case _: NoSuchElementException => Left(BufferUnderflow)
      }

    // 1.5.5 Variable Byte Integer
    def decodeVariableByteInteger(): Either[DecodeError, VariableByteInteger] =
      try {
        val l0 = v.getByte & 0xff
        val l1 = if ((l0 & 0x80) == 0x80) v.getByte & 0xff else 0
        val l2 = if ((l1 & 0x80) == 0x80) v.getByte & 0xff else 0
        val l3 = if ((l2 & 0x80) == 0x80) v.getByte & 0xff else 0
        val l = (l3 << 21) | ((l2 & 0x7f) << 14) | ((l1 & 0x7f) << 7) | (l0 & 0x7f)
        Right(VariableByteInteger(l))
      } catch {
        case _: NoSuchElementException => Left(BufferUnderflow)
      }

    // 1.5.6 Binary Data
    def decodeBinaryData(): Either[DecodeError, BinaryData] =
      try {
        val length = v.getShort & 0xffff
        Right(BinaryData(v.getBytes(length).toIndexedSeq))
      } catch {
        case _: NoSuchElementException => Left(BufferUnderflow)
      }

    // 1.5.7 UTF-8 String Pair
    def decodeStringPair(): Either[DecodeError, (String, String)] =
      try {
        for {
          name <- v.decodeString()
          value <- v.decodeString()
        } yield (name, value)
      } catch {
        case _: NoSuchElementException => Left(BufferUnderflow)
      }

    // 2 MQTT Control Packet format
    def decodeControlPacket(maxPacketSize: Int, protocolLevel: Connect.ProtocolLevel): Either[DecodeError, ControlPacket] =
      try {
        val b = v.getByte & 0xff
        v.decodeVariableByteInteger() match {
          case Right(VariableByteInteger(l)) if l <= maxPacketSize =>
            (ControlPacketType(b >> 4), ControlPacketFlags(b & 0xf)) match {
              case (ControlPacketType.Reserved, ControlPacketFlags.ReservedGeneral) =>
                Right(Reserved)
              case (ControlPacketType.CONNECT, ControlPacketFlags.ReservedGeneral) =>
                v.decodeConnect()
              case (ControlPacketType.CONNACK, ControlPacketFlags.ReservedGeneral) =>
                v.decodeConnAck(protocolLevel)
              case (ControlPacketType.PUBLISH, flags) =>
                v.decodePublish(l, flags, protocolLevel)
              case (ControlPacketType.PUBACK, ControlPacketFlags.ReservedGeneral) =>
                v.decodePubAck(l)
              case (ControlPacketType.PUBREC, ControlPacketFlags.ReservedGeneral) =>
                v.decodePubRec(l)
              case (ControlPacketType.PUBREL, ControlPacketFlags.ReservedPubRel) =>
                v.decodePubRel(l)
              case (ControlPacketType.PUBCOMP, ControlPacketFlags.ReservedGeneral) =>
                v.decodePubComp(l)
              case (ControlPacketType.SUBSCRIBE, ControlPacketFlags.ReservedSubscribe) =>
                v.decodeSubscribe(protocolLevel)
              case (ControlPacketType.SUBACK, ControlPacketFlags.ReservedGeneral) =>
                v.decodeSubAck(protocolLevel)
              case (ControlPacketType.UNSUBSCRIBE, ControlPacketFlags.ReservedUnsubscribe) =>
                v.decodeUnsubscribe()
              case (ControlPacketType.UNSUBACK, ControlPacketFlags.ReservedUnsubAck) =>
                v.decodeUnsubAck()
              case (ControlPacketType.PINGREQ, ControlPacketFlags.ReservedGeneral) =>
                Right(PingReq)
              case (ControlPacketType.PINGRESP, ControlPacketFlags.ReservedGeneral) =>
                Right(PingResp)
              case (ControlPacketType.DISCONNECT, ControlPacketFlags.ReservedGeneral) =>
                v.decodeDisconnect(l)
              case (ControlPacketType.AUTH, ControlPacketFlags.ReservedGeneral) =>
                v.decodeAuth()
              case (packetType, flags) =>
                Left(UnknownPacketType(packetType, flags))
            }
          case Right(VariableByteInteger(l)) =>
            Left(InvalidPacketSize(l, maxPacketSize))
          case Left(BufferUnderflow) => Left(BufferUnderflow)
        }
      } catch {
        case _: NoSuchElementException => Left(BufferUnderflow)
      }

    // 3.1 CONNECT – Client requests a connection to a Server
    def decodeConnect(): Either[DecodeError, Connect] =
      try {
        val protocolName = v.decodeString()
        val protocolLevel = v.getByte & 0xff
        protocolName match {
          case Right(Connect.Mqtt) =>
            if (List(Connect.v5, Connect.v311).contains(protocolLevel)) {
              val connectFlags = ConnectFlags(v.getByte & 0xff)
              if (!connectFlags.contains(ConnectFlags.Reserved)) {
                (for {
                  keepAlive <- v.decodeTwoByteInteger().map(FiniteDuration(_, TimeUnit.SECONDS))
                  props <- if (protocolLevel==Connect.v5) v.decodeConnectProperties() else Right(ConnectProperties())
                  payload <- v.decodeConnectPayload(
                    connectFlags.contains(ConnectFlags.WillFlag),
                    connectFlags.contains(ConnectFlags.UsernameFlag),
                    connectFlags.contains(ConnectFlags.PasswordFlag),
                    protocolLevel
                  )
                } yield Connect(Connect.Mqtt, protocolLevel, connectFlags, keepAlive, props, payload))
                  .left.map(BadConnectMessage)
              } else {
                Left(ConnectFlagReservedSet)
              }
            }
            else
              Left(UnknownConnectProtocol(protocolName, protocolLevel))
          case _ =>
            Left(UnknownConnectProtocol(protocolName, protocolLevel))
        }
      } catch {
        case _: NoSuchElementException => Left(BadConnectMessage(BufferUnderflow))
      }

    // 3.1.2.11 CONNECT Properties
    def decodeConnectProperties(): Either[DecodeError, ConnectProperties] =
      try {
        val length = v.decodeVariableByteInteger()
        length.flatMap(l => {
          val propBytesArray = Array.fill(l.underlying)(0.toByte)
          v.copyToArray(propBytesArray)
          val propBytes = ByteArrayIterator(propBytesArray)
          @tailrec
          def iterate(props: ConnectProperties): Either[DecodeError, ConnectProperties] = {
            if (propBytes.isEmpty)
              Right(props)
            else {
              val propId = propBytes.decodeVariableByteInteger()
              propId match {
                case Right(PropertyIdentifiers.SessionExpiryInterval) =>
                  if (props.SessionExpiryInterval.isEmpty)
                    propBytes.decodeFourByteInteger() match {
                      case Right(value) =>
                        iterate(props.copy(SessionExpiryInterval = Some(value)))
                      case Left(e) => Left(e)
                    }
                  else
                    Left(SessionExpiryIntervalDefinedMoreThanOnce)
                case Right(PropertyIdentifiers.ReceiveMaximum) =>
                  if (props.ReceiveMaximum.isEmpty)
                    propBytes.decodeTwoByteInteger() match {
                      case Right(value) =>
                        iterate(props.copy(ReceiveMaximum = Some(value)))
                      case Left(e) => Left(e)
                    }
                  else
                    Left(ReceiveMaximumDefinedMoreThanOnce)
                case Right(PropertyIdentifiers.MaximumPacketSize) =>
                  if (props.MaximumPacketSize.isEmpty)
                    propBytes.decodeFourByteInteger() match {
                      case Right(value) =>
                        iterate(props.copy(MaximumPacketSize = Some(value)))
                      case Left(e) => Left(e)
                    }
                  else
                    Left(ReceiveMaximumDefinedMoreThanOnce)
                case Right(PropertyIdentifiers.TopicAliasMaximum) =>
                  if (props.TopicAliasMaximum.isEmpty)
                    propBytes.decodeTwoByteInteger() match {
                      case Right(value) =>
                        iterate(props.copy(TopicAliasMaximum = Some(value)))
                      case Left(e) => Left(e)
                    }
                  else
                    Left(TopicAliasDefinedMoreThanOnce)
                case Right(PropertyIdentifiers.RequestResponseInformation) =>
                  if (props.RequestResponseInformation.isEmpty) {
                    val b = propBytes.getByte
                    if (b != 0x0 && b != 0x1)
                      Left(UnvalidRequestResponseInformationValue(b.toInt))
                    else
                      iterate(props.copy(RequestResponseInformation = Some(b)))
                  }
                  else
                    Left(RequestResponseInformationDefinedMoreThanOnce)
                case Right(PropertyIdentifiers.RequestProblemInformation) =>
                  if (props.RequestProblemInformation.isEmpty) {
                    val b = propBytes.getByte
                    if (b != 0x0 && b != 0x1)
                      Left(UnvalidRequestProblemInformationValue(b.toInt))
                    else
                      iterate(props.copy(RequestProblemInformation = Some(b)))
                  } else
                    Left(RequestProblemInformationDefinedMoreThanOnce)
                case Right(PropertyIdentifiers.UserProperty) =>
                  propBytes.decodeStringPair() match {
                    case Right(userProp) =>
                      val currentUserProps = props.UserProperties
                      val newUserProps = currentUserProps.fold(List(userProp))(_ :+ userProp)
                      iterate(props.copy(UserProperties = Some(newUserProps)))
                    case Left(e) => Left(e)
                  }
                case Right(PropertyIdentifiers.AuthenticationMethod) =>
                  if (props.AuthenticationMethod.isEmpty)
                    propBytes.decodeString() match {
                      case Right(str) =>
                        iterate(props.copy(AuthenticationMethod = Some(str)))
                      case Left(e) => Left(e)
                    }
                  else
                    Left(AuthenticationMethodDefinedMoreThanOnce)
                case Right(PropertyIdentifiers.AuthenticationData) =>
                  if (props.AuthenticationData.isEmpty)
                    propBytes.decodeBinaryData() match {
                      case Right(binaryData) =>
                        iterate(props.copy(AuthenticationData = Some(binaryData)))
                      case Left(e) => Left(e)
                    }
                  else
                    Left(AuthenticationDataDefinedMoreThanOnce)
                case Right(id) =>
                  Left(UnknownPropertyId(id))
                case Left(e) => Left(e)
              }
            }
          }
          iterate(ConnectProperties()) match {
            case Right(connectProperties) =>
              if (connectProperties.AuthenticationMethod.isEmpty && connectProperties.AuthenticationData.isDefined)
                Left(AuthDataDefinedWhileAuthMethodUndefined)
              else
                Right(connectProperties)
            case Left(err) => Left(BadConnectProperties(err))
          }
        })
      } catch {
        case _: NoSuchElementException => Left(BufferUnderflow)
      }

    // 3.1.3 CONNECT Payload
    def decodeConnectPayload(willFlag: Boolean, usernameFlag: Boolean, passwordFlag: Boolean, protocolLevel: Connect.ProtocolLevel=Connect.v5): Either[DecodeError, ConnectPayload] =
      try {
        (for {
          clientId <- v.decodeString()
          willProps <- if (willFlag)
            if (protocolLevel==Connect.v5)
              v.decodeConnectPayloadWillProperties().map(Some(_))
            else
              Right(Some(ConnectPayloadWillProperties()))
          else
            Right(None)
          willTopic <- if (willFlag) v.decodeString().map(Some(_)) else Right(None)
          willPayload <- if (willFlag) v.decodeBinaryData().map(Some(_)) else Right(None)
          username <- if (usernameFlag) v.decodeString().map(Some(_)) else Right(None)
          password <- if (passwordFlag) v.decodeString().map(Some(_)) else Right(None)
        } yield ConnectPayload(clientId, willProps, willTopic, willPayload, username, password))
          .left.map(BadConnectPayload)
      } catch {
        case _: NoSuchElementException => Left(BufferUnderflow)
      }

    // 3.1.3.2 CONNECT Payload Will Properties
    def decodeConnectPayloadWillProperties(): Either[DecodeError, ConnectPayloadWillProperties] =
      try {
        val length = v.decodeVariableByteInteger()
        length.flatMap (l => {
          val propBytesArray = Array.fill(l.underlying)(0.toByte)
          v.copyToArray(propBytesArray)
          val propBytes = ByteArrayIterator(propBytesArray)
          @tailrec
          def iterate(props: ConnectPayloadWillProperties): Either[DecodeError, ConnectPayloadWillProperties] = {
            if (propBytes.isEmpty)
              Right(props)
            else {
              val propId = propBytes.decodeVariableByteInteger()
              propId match {
                case Right(PropertyIdentifiers.WillDelayInterval) =>
                  if (props.WillDelayInterval.isEmpty)
                    propBytes.decodeFourByteInteger() match {
                      case Right(value) =>
                        iterate(props.copy(WillDelayInterval = Some(value)))
                      case Left(e) => Left(e)
                    }
                  else
                    Left(WillDelayIntervalDefinedMoreThanOnce)
                case Right(PropertyIdentifiers.PayloadFormatIndicator) =>
                  if (props.PayloadFormatIndicator.isEmpty) {
                    val b = propBytes.getByte
                    if (b != 0x0 && b != 0x1)
                      Left(UnvalidPayloadFormatIndicatorValue(b.toInt))
                    else
                      iterate(props.copy(PayloadFormatIndicator = Some(b)))
                  }
                  else
                    Left(PayloadFormatIndicatorDefinedMoreThanOnce)
                case Right(PropertyIdentifiers.MessageExpiryInterval) =>
                  if (props.MessageExpiryInterval.isEmpty)
                    propBytes.decodeFourByteInteger() match {
                      case Right(value) =>
                        iterate(props.copy(MessageExpiryInterval = Some(value)))
                      case Left(e) => Left(e)
                    }
                  else
                    Left(MessageExpiryIntervalDefinedMoreThanOnce)
                case Right(PropertyIdentifiers.ContentType) =>
                  if (props.ContentType.isEmpty)
                    propBytes.decodeString() match {
                      case Right(value) =>
                        iterate(props.copy(ContentType = Some(value)))
                      case Left(e) => Left(e)
                    }
                  else
                    Left(ContentTypeDefinedMoreThanOnce)
                case Right(PropertyIdentifiers.ResponseTopic) =>
                  if (props.ResponseTopic.isEmpty)
                    propBytes.decodeString() match {
                      case Right(value) =>
                        iterate(props.copy(ResponseTopic = Some(value)))
                      case Left(e) => Left(e)
                    }
                  else
                    Left(ResponseTopicDefinedMoreThanOnce)
                case Right(PropertyIdentifiers.CorrelationData) =>
                  if (props.CorrelationData.isEmpty)
                    propBytes.decodeBinaryData() match {
                      case Right(value) =>
                        iterate(props.copy(CorrelationData = Some(value)))
                      case Left(e) => Left(e)
                    }
                  else
                    Left(CorrelationDataDefinedMoreThanOnce)
                case Right(PropertyIdentifiers.UserProperty) =>
                  propBytes.decodeStringPair() match {
                    case Right(userProp) =>
                      val currentUserProps = props.UserProperties
                      val newUserProps = currentUserProps.fold(List(userProp))(_ :+ userProp)
                      iterate(props.copy(UserProperties = Some(newUserProps)))
                    case Left(e) => Left(e)
                  }
                case Right(id) =>
                  Left(UnknownPropertyId(id))
                case Left(e) => Left(e)
              }
            }
          }
          iterate(ConnectPayloadWillProperties())
            .left.map(BadConnectPayloadWillProperties)

        })
      } catch {
        case _: NoSuchElementException => Left(BufferUnderflow)
      }

    // 3.2 CONNACK – Acknowledge connection request
    def decodeConnAck(protocolLevel: Connect.ProtocolLevel): Either[DecodeError, ConnAck] =
      try {
        val flags = ConnAckFlags(v.getByte & 0xff)
        if ((flags.underlying & 0xfe) == 0) {
          val reasonCode = ConnAckReasonCode(v.getByte & 0xff)
          (for {
            props <- if (protocolLevel==Connect.v5) v.decodeConnAckProperties() else Right(ConnAckProperties())
          } yield ConnAck(flags, reasonCode, props))
            .left.map(BadConnAckMessage)
        } else
          Left(ConnectAckFlagReservedBitsSet)
      } catch {
        case _: NoSuchElementException => Left(BadConnAckMessage(BufferUnderflow))
      }

    // 3.2.2.3 CONNACK Properties
    def decodeConnAckProperties(): Either[DecodeError, ConnAckProperties] =
      try {
        val length = v.decodeVariableByteInteger()
        length.flatMap(l => {
          val propBytesArray = Array.fill(l.underlying)(0.toByte)
          v.copyToArray(propBytesArray)
          val propBytes = ByteArrayIterator(propBytesArray)
          @tailrec
          def iterate(props: ConnAckProperties): Either[DecodeError, ConnAckProperties] = {
            if (propBytes.isEmpty)
              Right(props)
            else {
              val propId = propBytes.decodeVariableByteInteger()
              propId match {
                case Right(PropertyIdentifiers.SessionExpiryInterval) =>
                  if (props.SessionExpiryInterval.isEmpty)
                    propBytes.decodeFourByteInteger() match {
                      case Right(value) =>
                        iterate(props.copy(SessionExpiryInterval = Some(value)))
                      case Left(e) => Left(e)
                    }
                  else
                    Left(SessionExpiryIntervalDefinedMoreThanOnce)
                case Right(PropertyIdentifiers.ReceiveMaximum) =>
                  if (props.ReceiveMaximum.isEmpty)
                    propBytes.decodeTwoByteInteger() match {
                      case Right(value) =>
                        iterate(props.copy(ReceiveMaximum = Some(value)))
                      case Left(e) => Left(e)
                    }
                  else
                    Left(ReceiveMaximumDefinedMoreThanOnce)
                case Right(PropertyIdentifiers.MaximumQoS) =>
                  if (props.MaximumQoS.isEmpty) {
                    val b = propBytes.getByte
                    if (b != 0x0 && b != 0x1)
                      Left(UnvalidMaximumQosValue(b.toInt))
                    else
                      iterate(props.copy(MaximumQoS = Some(b)))
                  }
                  else
                    Left(MaximumQosDefinedMoreThanOnce)
                case Right(PropertyIdentifiers.RetainAvailable) =>
                  if (props.RetainAvailable.isEmpty) {
                    val b = propBytes.getByte
                    if (b != 0x0 && b != 0x1)
                      Left(UnvalidRetainAvailableValue(b.toInt))
                    else
                      iterate(props.copy(RetainAvailable = Some(b)))
                  }
                  else
                    Left(RetainAvailableDefinedMoreThanOnce)
                case Right(PropertyIdentifiers.MaximumPacketSize) =>
                  if (props.MaximumPacketSize.isEmpty)
                    propBytes.decodeFourByteInteger() match {
                      case Right(value) =>
                        iterate(props.copy(MaximumPacketSize = Some(value)))
                      case Left(e) => Left(e)
                    }
                  else
                    Left(ReceiveMaximumDefinedMoreThanOnce)
                case Right(PropertyIdentifiers.AssignedClientIdentifier) =>
                  if (props.AssignedClientIdentifier.isEmpty)
                    propBytes.decodeString() match {
                      case Right(str) =>
                        iterate(props.copy(AssignedClientIdentifier = Some(str)))
                      case Left(e) => Left(e)
                    }
                  else {
                    Left(AssignedClientIdentifierDefinedMoreThanOnce)
                  }
                case Right(PropertyIdentifiers.TopicAliasMaximum) =>
                  if (props.TopicAliasMaximum.isEmpty)
                    propBytes.decodeTwoByteInteger() match {
                      case Right(value) =>
                        iterate(props.copy(TopicAliasMaximum = Some(value)))
                      case Left(e) => Left(e)
                    }
                  else
                    Left(TopicAliasMaximumDefinedMoreThanOnce)
                case Right(PropertyIdentifiers.ReasonString) =>
                  propBytes.decodeString() match {
                    case Right(str) =>
                      iterate(props.copy(ReasonString = Some(str)))
                    case Left(e) => Left(e)
                  }
                case Right(PropertyIdentifiers.UserProperty) =>
                  propBytes.decodeStringPair() match {
                    case Right(userProp) =>
                      val currentUserProps = props.UserProperties
                      val newUserProps = currentUserProps.fold(List(userProp))(_ :+ userProp)
                      iterate(props.copy(UserProperties = Some(newUserProps)))
                    case Left(e) => Left(e)
                  }
                case Right(PropertyIdentifiers.WildcardSubscriptionAvailable) =>
                  if (props.WildcardSubscriptionAvailable.isEmpty) {
                    val b = propBytes.getByte
                    if (b != 0x0 && b != 0x1)
                      Left(UnvalidWildcardSubscriptionAvailableValue(b.toInt))
                    else
                      iterate(props.copy(WildcardSubscriptionAvailable = Some(b)))
                  }
                  else
                    Left(WildcardSubscriptionAvailableDefinedMoreThanOnce)
                case Right(PropertyIdentifiers.SubscriptionIdentifierAvailable) =>
                  if (props.SubscriptionIdentifierAvailable.isEmpty) {
                    val b = propBytes.getByte
                    if (b != 0x0 && b != 0x1)
                      Left(UnvalidSubscriptionIdentifierAvailableValue(b.toInt))
                    else
                      iterate(props.copy(SubscriptionIdentifierAvailable = Some(b)))
                  }
                  else
                    Left(SubscriptionIdentifierAvailableDefinedMoreThanOnce)
                case Right(PropertyIdentifiers.SharedSubscriptionAvailable) =>
                  if (props.SharedSubscriptionAvailable.isEmpty) {
                    val b = propBytes.getByte
                    if (b != 0x0 && b != 0x1)
                      Left(UnvalidSharedSubscriptionAvailableValue(b.toInt))
                    else
                      iterate(props.copy(SharedSubscriptionAvailable = Some(b)))
                  }
                  else
                    Left(SharedSubscriptionAvailableDefinedMoreThanOnce)
                case Right(PropertyIdentifiers.ServerKeepAlive) =>
                  if (props.ServerKeepAlive.isEmpty)
                    propBytes.decodeTwoByteInteger() match {
                      case Right(value) =>
                        iterate(props.copy(ServerKeepAlive = Some(value)))
                      case Left(e) => Left(e)
                    }
                  else
                    Left(ServerKeepAliveDefinedMoreThanOnce)
                case Right(PropertyIdentifiers.ResponseInformation) =>
                  if (props.ResponseInformation.isEmpty)
                    propBytes.decodeString() match {
                      case Right(str) =>
                        iterate(props.copy(ResponseInformation = Some(str)))
                      case Left(e) => Left(e)
                    }
                  else {
                    Left(ResponseInformationDefinedMoreThanOnce)
                  }
                case Right(PropertyIdentifiers.ServerReference) =>
                  if (props.ServerReference.isEmpty)
                    propBytes.decodeString() match {
                      case Right(str) =>
                        iterate(props.copy(ServerReference = Some(str)))
                      case Left(e) => Left(e)
                    }
                  else {
                    Left(ServerReferenceDefinedMoreThanOnce)
                  }
                case Right(PropertyIdentifiers.AuthenticationMethod) =>
                  if (props.AuthenticationMethod.isEmpty)
                    propBytes.decodeString() match {
                      case Right(str) =>
                        iterate(props.copy(AuthenticationMethod = Some(str)))
                      case Left(e) => Left(e)
                    }
                  else
                    Left(AuthenticationMethodDefinedMoreThanOnce)
                case Right(PropertyIdentifiers.AuthenticationData) =>
                  if (props.AuthenticationData.isEmpty)
                    propBytes.decodeBinaryData() match {
                      case Right(binaryData) =>
                        iterate(props.copy(AuthenticationData = Some(binaryData)))
                      case Left(e) => Left(e)
                    }
                  else
                    Left(AuthenticationDataDefinedMoreThanOnce)
                case Right(id) =>
                  Left(UnknownPropertyId(id))
                case Left(e) => Left(e)
              }
            }
          }
          iterate(ConnAckProperties())
            .left.map(BadConnAckProperties)
        })
      } catch {
        case _: NoSuchElementException => Left(BufferUnderflow)
      }

    // 3.3 PUBLISH – Publish message
    def decodePublish(l: Int, flags: ControlPacketFlags, protocolLevel: Connect.ProtocolLevel): Either[DecodeError, Publish] =
      try {
        if (!flags.contains(ControlPacketFlags.QoSReserved)) {
          val packetLen = v.len
          (for {
            topicName <- v.decodeString()
            packetId <- if (
              flags.contains(ControlPacketFlags.QoSAtLeastOnceDelivery) ||
                flags.contains(ControlPacketFlags.QoSExactlyOnceDelivery)
            ) v.decodeTwoByteInteger().map(id => Some(PacketId(id)))
            else Right(None)
            properties <- if (protocolLevel == Connect.v5) v.decodePublishProperties() else Right(PublishProperties())
            payload <- Right(v.getByteString(l - (packetLen - v.len)))
          } yield Publish(flags, topicName, packetId, properties, payload))
            .left.map(BadPublishMessage)
        } else {
          Left(InvalidQoSFlag)
        }
      } catch {
        case _: NoSuchElementException => Left(BadPublishMessage(BufferUnderflow))
      }

    // 3.3.2.3 Publish Properties
    def decodePublishProperties(): Either[DecodeError, PublishProperties] =
      try {
        val length = v.decodeVariableByteInteger()
        length.flatMap(l => {
          val propBytesArray = Array.fill(l.underlying)(0.toByte)
          v.copyToArray(propBytesArray)
          val propBytes = ByteArrayIterator(propBytesArray)
          @tailrec
          def iterate(props: PublishProperties): Either[DecodeError, PublishProperties] = {
            if (propBytes.isEmpty)
              Right(props)
            else {
              val propId = propBytes.decodeVariableByteInteger()
              propId match {
                case Right(PropertyIdentifiers.PayloadFormatIndicator) =>
                  if (props.PayloadFormatIndicator.isEmpty) {
                    val b = propBytes.getByte
                    if (b != 0x0 && b != 0x1)
                      Left(UnvalidPayloadFormatIndicatorValue(b.toInt))
                    else
                      iterate(props.copy(PayloadFormatIndicator = Some(b)))
                  }
                  else
                    Left(PayloadFormatIndicatorDefinedMoreThanOnce)
                case Right(PropertyIdentifiers.MessageExpiryInterval) =>
                  if (props.MessageExpiryInterval.isEmpty)
                    propBytes.decodeFourByteInteger() match {
                      case Right(value) =>
                        iterate(props.copy(MessageExpiryInterval = Some(value)))
                      case Left(e) => Left(e)
                    }
                  else
                    Left(MessageExpiryIntervalDefinedMoreThanOnce)
                case Right(PropertyIdentifiers.TopicAlias) =>
                  if (props.TopicAlias.isEmpty)
                    propBytes.decodeTwoByteInteger() match {
                      case Right(value) =>
                        iterate(props.copy(TopicAlias = Some(value)))
                      case Left(e) => Left(e)
                    }
                  else
                    Left(TopicAliasDefinedMoreThanOnce)
                case Right(PropertyIdentifiers.ResponseTopic) =>
                  if (props.ResponseTopic.isEmpty)
                    propBytes.decodeString() match {
                      case Right(value) =>
                        iterate(props.copy(ResponseTopic = Some(value)))
                      case Left(e) => Left(e)
                    }
                  else
                    Left(ResponseTopicDefinedMoreThanOnce)
                case Right(PropertyIdentifiers.CorrelationData) =>
                  if (props.CorrelationData.isEmpty) {
                    propBytes.decodeBinaryData() match {
                      case Right(value) =>
                        iterate(props.copy(CorrelationData = Some(value)))
                      case Left(e) => Left(e)
                    }
                  }
                  else
                    Left(CorrelationDataDefinedMoreThanOnce)
                case Right(PropertyIdentifiers.UserProperty) =>
                  propBytes.decodeStringPair() match {
                    case Right(userProp) =>
                      val currentUserProps = props.UserProperties
                      val newUserProps = currentUserProps.fold(List(userProp))(_ :+ userProp)
                      iterate(props.copy(UserProperties = Some(newUserProps)))
                    case Left(e) => Left(e)
                  }
                case Right(PropertyIdentifiers.SubscriptionIdentifier) =>
                  if (props.SubscriptionIdentifier.isEmpty)
                    propBytes.decodeVariableByteInteger() match {
                      case Right(value) =>
                        iterate(props.copy(SubscriptionIdentifier = Some(value.underlying)))
                      case Left(e) => Left(e)
                    }
                  else
                    Left(SubscriptionIdentifierDefinedMoreThanOnce)
                case Right(PropertyIdentifiers.ContentType) =>
                  if (props.ContentType.isEmpty)
                    propBytes.decodeString() match {
                      case Right(str) =>
                        iterate(props.copy(ContentType = Some(str)))
                      case Left(e) => Left(e)
                    }
                  else
                    Left(CotentTypeDefinedMoreThanOnce)
                case Right(id) =>
                  Left(UnknownPropertyId(id))
                case Left(e) => Left(e)
              }
            }
          }
          iterate(PublishProperties())
            .left.map(BadPublishProperties)
        })
      } catch {
        case _: NoSuchElementException => Left(BufferUnderflow)
      }

    // 3.4 PUBACK – Publish acknowledgement
    def decodePubAck(l: Int): Either[DecodeError, PubAck] =
      try {
        (for {
          packetId <- v.decodeTwoByteInteger().map(PacketId)
          reasonCode <- if (l>2) Right(PubAckReasonCode(v.getByte.toInt & 0xff)) else Right(PubAckReasonCode(0))
          pubAckProps <- if (l>3) v.decodePubAckProperties().map(Some(_)) else Right(None)
        } yield PubAck(packetId, reasonCode, pubAckProps))
          .left.map(BadPubAckMessage)
      } catch {
        case _: NoSuchElementException => Left(BadPubAckMessage(BufferUnderflow))
      }

    // 3.4.1.1 PUBACK Properties
    def decodePubAckProperties(): Either[DecodeError, PubAckProperties] =
      try {
        val length = v.decodeVariableByteInteger()
        length.flatMap(l => {
          val propBytesArray = Array.fill(l.underlying)(0.toByte)
          v.copyToArray(propBytesArray)
          val propBytes = ByteArrayIterator(propBytesArray)
          @tailrec
          def iterate(props: PubAckProperties): Either[DecodeError, PubAckProperties] = {
            if (propBytes.isEmpty)
              Right(props)
            else {
              val propId = propBytes.decodeVariableByteInteger()
              propId match {
                case Right(PropertyIdentifiers.ReasonString) =>
                  if (props.ReasonString.isEmpty)
                    propBytes.decodeString() match {
                      case Right(value) =>
                        iterate(props.copy(ReasonString = Some(value)))
                      case Left(e) => Left(e)
                    }
                  else
                    Left(ResponseTopicDefinedMoreThanOnce)
                case Right(PropertyIdentifiers.UserProperty) =>
                  propBytes.decodeStringPair() match {
                    case Right(userProp) =>
                      val currentUserProps = props.UserProperties
                      val newUserProps = currentUserProps.fold(List(userProp))(_ :+ userProp)
                      iterate(props.copy(UserProperties = Some(newUserProps)))
                    case Left(e) => Left(e)
                  }
                case Right(id) =>
                  Left(UnknownPropertyId(id))
                case Left(e) => Left(e)
              }
            }
          }
          iterate(PubAckProperties())
            .left.map(BadPubAckProperties)
        })
      } catch {
        case _: NoSuchElementException => Left(BufferUnderflow)
      }

    // 3.5 PUBREC – Publish received (QoS 2 publish received, part 1)
    def decodePubRec(l: Int): Either[DecodeError, PubRec] =
      try {
        (for {
          packetId <- v.decodeTwoByteInteger().map(PacketId)
          reasonCode <- if (l>2) Right(PubRecReasonCode(v.getByte.toInt & 0xff)) else Right(PubRecReasonCode(0))
          pubRecProps <- if (l>3) v.decodePubRecProperties().map(Some(_)) else Right(Some(PubRecProperties()))
        } yield PubRec(packetId, reasonCode, pubRecProps))
          .left.map(BadPubRecMessage)
      } catch {
        case _: NoSuchElementException => Left(BadPubRecMessage(BufferUnderflow))
      }

    // 3.4. PUBREC Properties
    def decodePubRecProperties(): Either[DecodeError, PubRecProperties] =
      try {
        val length = v.decodeVariableByteInteger()
        length.flatMap(l => {
          val propBytesArray = Array.fill(l.underlying)(0.toByte)
          v.copyToArray(propBytesArray)
          val propBytes = ByteArrayIterator(propBytesArray)
          @tailrec
          def iterate(props: PubRecProperties): Either[DecodeError, PubRecProperties] = {
            if (propBytes.isEmpty)
              Right(props)
            else {
              val propId = propBytes.decodeVariableByteInteger()
              propId match {
                case Right(PropertyIdentifiers.ReasonString) =>
                  if (props.ReasonString.isEmpty)
                    propBytes.decodeString() match {
                      case Right(value) =>
                        iterate(props.copy(ReasonString = Some(value)))
                      case Left(e) => Left(e)
                    }
                  else
                    Left(ResponseTopicDefinedMoreThanOnce)
                case Right(PropertyIdentifiers.UserProperty) =>
                  propBytes.decodeStringPair() match {
                    case Right(userProp) =>
                      val currentUserProps = props.UserProperties
                      val newUserProps = currentUserProps.fold(List(userProp))(_ :+ userProp)
                      iterate(props.copy(UserProperties = Some(newUserProps)))
                    case Left(e) => Left(e)
                  }
                case Right(id) =>
                  Left(UnknownPropertyId(id))
                case Left(e) => Left(e)
              }
            }
          }
          iterate(PubRecProperties())
            .left.map(BadPubRecProperties)
        })
      } catch {
        case _: NoSuchElementException => Left(BufferUnderflow)
      }

    // 3.6 PUBREL – Publish release (QoS 2 publish received, part 2)
    def decodePubRel(l: Int): Either[DecodeError, PubRel] =
      try {
        (for {
          packetId <- v.decodeTwoByteInteger().map(PacketId)
          reasonCode <- if (l>2) Right(PubRelReasonCode(v.getByte.toInt & 0xff)) else Right(PubRelReasonCode(0))
          pubRelProps <- if (l>3) v.decodePubRelProperties().map(Some(_)) else Right(Some(PubRelProperties()))
        } yield PubRel(packetId, reasonCode, pubRelProps))
          .left.map(BadPubRelMessage)
      } catch {
        case _: NoSuchElementException => Left(BadPubRelMessage(BufferUnderflow))
      }

    // 3.6.2.2 PUBREL Properties
    def decodePubRelProperties(): Either[DecodeError, PubRelProperties] =
      try {
        val length = v.decodeVariableByteInteger()
        length.flatMap(l => {
          val propBytesArray = Array.fill(l.underlying)(0.toByte)
          v.copyToArray(propBytesArray)
          val propBytes = ByteArrayIterator(propBytesArray)
          @tailrec
          def iterate(props: PubRelProperties): Either[DecodeError, PubRelProperties] = {
            if (propBytes.isEmpty)
              Right(props)
            else {
              val propId = propBytes.decodeVariableByteInteger()
              propId match {
                case Right(PropertyIdentifiers.ReasonString) =>
                  if (props.ReasonString.isEmpty)
                    propBytes.decodeString() match {
                      case Right(value) =>
                        iterate(props.copy(ReasonString = Some(value)))
                      case Left(e) => Left(e)
                    }
                  else
                    Left(ResponseTopicDefinedMoreThanOnce)
                case Right(PropertyIdentifiers.UserProperty) =>
                  propBytes.decodeStringPair() match {
                    case Right(userProp) =>
                      val currentUserProps = props.UserProperties
                      val newUserProps = currentUserProps.fold(List(userProp))(_ :+ userProp)
                      iterate(props.copy(UserProperties = Some(newUserProps)))
                    case Left(e) => Left(e)
                  }
                case Right(id) =>
                  Left(UnknownPropertyId(id))
                case Left(e) => Left(e)
              }
            }
          }
          iterate(PubRelProperties())
            .left.map(BadPubRelProperties)
        })
      } catch {
        case _: NoSuchElementException => Left(BufferUnderflow)
      }

    // 3.7 PUBCOMP – Publish complete (QoS 2 publish received, part 3)
    def decodePubComp(l: Int): Either[DecodeError, PubComp] =
      try {
        (for {
          packetId <- v.decodeTwoByteInteger().map(PacketId)
          reasonCode <- if (l>2) Right(PubCompReasonCode(v.getByte.toInt & 0xff)) else Right(PubCompReasonCode(0))
          pubCompProps <- if (l>3) v.decodePubCompProperties().map(Some(_)) else Right(Some(PubCompProperties()))
        } yield PubComp(packetId, reasonCode, pubCompProps))
          .left.map(BadPubCompMessage)
      } catch {
        case _: NoSuchElementException => Left(BadPubCompMessage(BufferUnderflow))
      }

    // 3.7.2.2 PUBCOMP Properties
    def decodePubCompProperties(): Either[DecodeError, PubCompProperties] =
      try {
        val length = v.decodeVariableByteInteger()
        length.flatMap(l => {
          val propBytesArray = Array.fill(l.underlying)(0.toByte)
          v.copyToArray(propBytesArray)
          val propBytes = ByteArrayIterator(propBytesArray)
          @tailrec
          def iterate(props: PubCompProperties): Either[DecodeError, PubCompProperties] = {
            if (propBytes.isEmpty)
              Right(props)
            else {
              val propId = propBytes.decodeVariableByteInteger()
              propId match {
                case Right(PropertyIdentifiers.ReasonString) =>
                  if (props.ReasonString.isEmpty)
                    propBytes.decodeString() match {
                      case Right(value) =>
                        iterate(props.copy(ReasonString = Some(value)))
                      case Left(e) => Left(e)
                    }
                  else
                    Left(ResponseTopicDefinedMoreThanOnce)
                case Right(PropertyIdentifiers.UserProperty) =>
                  propBytes.decodeStringPair() match {
                    case Right(userProp) =>
                      val currentUserProps = props.UserProperties
                      val newUserProps = currentUserProps.fold(List(userProp))(_ :+ userProp)
                      iterate(props.copy(UserProperties = Some(newUserProps)))
                    case Left(e) => Left(e)
                  }
                case Right(id) =>
                  Left(UnknownPropertyId(id))
                case Left(e) => Left(e)
              }
            }
          }
          iterate(PubCompProperties())
            .left.map(BadPubCompProperties)
        })
      } catch {
        case _: NoSuchElementException => Left(BufferUnderflow)
      }

    // 3.8 SUBSCRIBE - Subscribe to topics
    def decodeSubscribe(protocolLevel: Connect.ProtocolLevel): Either[DecodeError, Subscribe] =
      try {
        @tailrec
        def decodeTopicFilters(topicFilters: Seq[(String, SubscribeOptions)]): Either[DecodeError, Seq[(String, SubscribeOptions)]] =
          if (v.isEmpty)
            Right(topicFilters)
          else
            v.decodeString() match {
              case Right(topicFilter) =>
                val options = v.getByte & 0xff
                if ((options & 0x03) == SubscribeOptions.QoSReserved.underlying)
                  Left(InvalidQoSFlag)
                else if (protocolLevel < Connect.v5) {
                  if ((options & 0xFC) > 0)
                    Left(SubscriptionOptionReservedBitSet)
                  else
                    decodeTopicFilters(topicFilters :+ ((topicFilter, SubscribeOptions(options))))
                }
                else {
                  if ((options & 0xC0) > 0)
                    Left(SubscriptionOptionReservedBitSet)
                  else if ((options & 0x30) == SubscribeOptions.RetainHandlingReserved.underlying)
                    Left(InvalidRetainHandlingFlag)
                  else decodeTopicFilters(topicFilters :+ ((topicFilter, SubscribeOptions(options))))
                }
              case Left(err) => Left(BadTopicFilter(err))
            }

        (for {
          packetId <- v.decodeTwoByteInteger().map(PacketId)
          props <- if (protocolLevel==Connect.v5) v.decodeSubscribeProperties() else Right(SubscribeProperties())
          topicFilters <- decodeTopicFilters(Seq()).flatMap {
            case Seq() => Left(EmptyTopicFilterListNotAllowed)
            case tf => Right(tf)
          }
        } yield Subscribe(packetId, props, topicFilters))
          .left.map(BadSubscribeMessage)
      } catch {
        case _: NoSuchElementException => Left(BadSubscribeMessage(BufferUnderflow))
      }

    // 3.8.2.2 SUBSCRIBE Properties
    def decodeSubscribeProperties(): Either[DecodeError, SubscribeProperties] =
      try {
        val length = v.decodeVariableByteInteger()
        length.flatMap(l => {
          val propBytesArray = Array.fill(l.underlying)(0.toByte)
          v.copyToArray(propBytesArray)
          val propBytes = ByteArrayIterator(propBytesArray)
          @tailrec
          def iterate(props: SubscribeProperties): Either[DecodeError, SubscribeProperties] = {
            if (propBytes.isEmpty)
              Right(props)
            else {
              val propId = propBytes.decodeVariableByteInteger()
              propId match {
                case Right(PropertyIdentifiers.SubscriptionIdentifier) =>
                  if (props.SubscriptionIdentifier.isEmpty)
                    propBytes.decodeVariableByteInteger() match {
                      case Right(value) =>
                        iterate(props.copy(SubscriptionIdentifier = Some(value.underlying)))
                      case Left(e) => Left(e)
                    }
                  else
                    Left(SubscriptionIdentifierDefinedMoreThanOnce)
                case Right(PropertyIdentifiers.UserProperty) =>
                  propBytes.decodeStringPair() match {
                    case Right(userProp) =>
                      val currentUserProps = props.UserProperties
                      val newUserProps = currentUserProps.fold(List(userProp))(_ :+ userProp)
                      iterate(props.copy(UserProperties = Some(newUserProps)))
                    case Left(e) => Left(e)
                  }
                case Right(id) =>
                  Left(UnknownPropertyId(id))
                case Left(e) => Left(e)
              }
            }
          }

          iterate(SubscribeProperties())
            .left.map(BadSubscribeProperties)
        })
      } catch {
        case _: NoSuchElementException => Left(BufferUnderflow)
      }

    // 3.9 SUBACK – Subscribe acknowledgement
    def decodeSubAck(protocolLevel: Connect.ProtocolLevel): Either[DecodeError, SubAck] =
      try {
        @tailrec
        def decodeReasonCodes(reasonCodes: Seq[SubAckReasonCode]): Either[DecodeError, Seq[SubAckReasonCode]] =
          if (v.isEmpty)
            Right(reasonCodes)
          else {
            val reasonCode = SubAckReasonCode(v.getByte & 0xff)
            decodeReasonCodes(reasonCodes :+ reasonCode)
          }
        (for {
          packetId <- v.decodeTwoByteInteger().map(PacketId)
          props <- if (protocolLevel == Connect.v5) v.decodeSubAckProperties() else Right(SubAckProperties())
          reasonCodes <- decodeReasonCodes(Seq())
        } yield SubAck(packetId, props, reasonCodes))
          .left.map(BadSubAckMessage)
      } catch {
        case _: NoSuchElementException => Left(BadSubAckMessage(BufferUnderflow))
      }

    // 3.9.2.2 SUBACK Properties
    def decodeSubAckProperties(): Either[DecodeError, SubAckProperties] =
      try {
        val length = v.decodeVariableByteInteger()
        length.flatMap(l => {
          val propBytesArray = Array.fill(l.underlying)(0.toByte)
          v.copyToArray(propBytesArray)
          val propBytes = ByteArrayIterator(propBytesArray)
          @tailrec
          def iterate(props: SubAckProperties): Either[DecodeError, SubAckProperties] = {
            if (propBytes.isEmpty)
              Right(props)
            else {
              val propId = propBytes.decodeVariableByteInteger()
              propId match {
                case Right(PropertyIdentifiers.ReasonString) =>
                  if (props.ReasonString.isEmpty)
                    propBytes.decodeString() match {
                      case Right(value) =>
                        iterate(props.copy(ReasonString = Some(value)))
                      case Left(e) => Left(e)
                    }
                  else
                    Left(ReasonStringDefinedMoreThanOnce)
                case Right(PropertyIdentifiers.UserProperty) =>
                  propBytes.decodeStringPair() match {
                    case Right(userProp) =>
                      val currentUserProps = props.UserProperties
                      val newUserProps = currentUserProps.fold(List(userProp))(_ :+ userProp)
                      iterate(props.copy(UserProperties = Some(newUserProps)))
                    case Left(e) => Left(e)
                  }
                case Right(id) =>
                  Left(UnknownPropertyId(id))
                case Left(e) => Left(e)
              }
            }
          }

          iterate(SubAckProperties())
            .left.map(BadSubscribeProperties)
        })
      } catch {
        case _: NoSuchElementException => Left(BufferUnderflow)
      }

    // 3.10 UNSUBSCRIBE – Unsubscribe request
    def decodeUnsubscribe(): Either[DecodeError, Unsubscribe] =
      try {
        @tailrec
        def decodeTopicFilters(topicFilters: Seq[String]): Either[DecodeError, Seq[String]] =
          if (v.isEmpty)
            Right(topicFilters)
          else
            v.decodeString() match {
              case Right(topicFilter) =>
                decodeTopicFilters(topicFilters :+ topicFilter)
              case Left(err) => Left(BadTopicFilter(err))
            }
        (for {
          packetId <- v.decodeTwoByteInteger().map(PacketId)
          props <- v.decodeUnsubscribeProperties()
          topicFilters <- decodeTopicFilters(Seq()).flatMap {
            case Seq() => Left(EmptyTopicFilterListNotAllowed)
            case tf => Right(tf)
          }
        } yield Unsubscribe(packetId, props, topicFilters))
          .left.map(BadUnsubscribeMessage)
      } catch {
        case _: NoSuchElementException => Left(BadUnsubscribeMessage(BufferUnderflow))
      }

    // 3.10.2.2 UNSUBSCRIBE Properties
    def decodeUnsubscribeProperties(): Either[DecodeError, UnsubscribeProperties] =
      try {
        val length = v.decodeVariableByteInteger()
        length.flatMap(l => {
          val propBytesArray = Array.fill(l.underlying)(0.toByte)
          v.copyToArray(propBytesArray)
          val propBytes = ByteArrayIterator(propBytesArray)
          @tailrec
          def iterate(props: UnsubscribeProperties): Either[DecodeError, UnsubscribeProperties] = {
            if (propBytes.isEmpty)
              Right(props)
            else {
              val propId = propBytes.decodeVariableByteInteger()
              propId match {
                case Right(PropertyIdentifiers.UserProperty) =>
                  propBytes.decodeStringPair() match {
                    case Right(userProp) =>
                      val currentUserProps = props.UserProperties
                      val newUserProps = currentUserProps.fold(List(userProp))(_ :+ userProp)
                      iterate(props.copy(UserProperties = Some(newUserProps)))
                    case Left(e) => Left(e)
                  }
                case Right(id) =>
                  Left(UnknownPropertyId(id))
                case Left(e) => Left(e)
              }
            }
          }
          iterate(UnsubscribeProperties())
            .left.map(BadUnsubscribeProperties)
        })
      } catch {
        case _: NoSuchElementException => Left(BufferUnderflow)
      }

    // 3.11 UNSUBACK – Unsubscribe acknowledgement
    def decodeUnsubAck(): Either[DecodeError, UnsubAck] =
      try {
        @tailrec
        def decodeReasonCodes(reasonCodes: Seq[UnsubAckReasonCode]): Either[DecodeError, Seq[UnsubAckReasonCode]] =
          if (v.isEmpty)
            Right(reasonCodes)
          else {
            val reasonCode = UnsubAckReasonCode(v.getByte & 0xff)
            decodeReasonCodes(reasonCodes :+ reasonCode)
          }
        (for {
          packetId <- v.decodeTwoByteInteger().map(PacketId)
          props <- v.decodeUnsubAckProperties()
          reasonCodes <- decodeReasonCodes(Seq())
        } yield UnsubAck(packetId, props, reasonCodes))
          .left.map(BadUnsubAckMessage)
      } catch {
        case _: NoSuchElementException => Left(BadUnsubAckMessage(BufferUnderflow))
      }

    // 3.11.2.2 UNSUBACK Properties
    def decodeUnsubAckProperties(): Either[DecodeError, UnsubAckProperties] =
      try {
        val length = v.decodeVariableByteInteger()
        length.flatMap(l => {
          val propBytesArray = Array.fill(l.underlying)(0.toByte)
          v.copyToArray(propBytesArray)
          val propBytes = ByteArrayIterator(propBytesArray)
          @tailrec
          def iterate(props: UnsubAckProperties): Either[DecodeError, UnsubAckProperties] = {
            if (propBytes.isEmpty)
              Right(props)
            else {
              val propId = propBytes.decodeVariableByteInteger()
              propId match {
                case Right(PropertyIdentifiers.ReasonString) =>
                  if (props.ReasonString.isEmpty)
                    propBytes.decodeString() match {
                      case Right(value) =>
                        iterate(props.copy(ReasonString = Some(value)))
                      case Left(e) => Left(e)
                    }
                  else
                    Left(ReasonStringDefinedMoreThanOnce)
                case Right(PropertyIdentifiers.UserProperty) =>
                  propBytes.decodeStringPair() match {
                    case Right(userProp) =>
                      val currentUserProps = props.UserProperties
                      val newUserProps = currentUserProps.fold(List(userProp))(_ :+ userProp)
                      iterate(props.copy(UserProperties = Some(newUserProps)))
                    case Left(e) => Left(e)
                  }
                case Right(id) =>
                  Left(UnknownPropertyId(id))
                case Left(e) => Left(e)
              }
            }
          }
          iterate(UnsubAckProperties())
            .left.map(BadUnsubAckProperties)
        })
      } catch {
        case _: NoSuchElementException => Left(BufferUnderflow)
      }

    // 3.14 DISCONNECT - Disconnect Notification
    def decodeDisconnect(l: Int): Either[DecodeError, Disconnect] =
      try {
        val reasonCode = if (l<1) DisconnectReasonCode(0) else DisconnectReasonCode(v.getByte & 0xff)
        (for {
          props <- if (l<2) Right(DisconnectProperties()) else v.decodeDisconnectProperties()
        } yield Disconnect(reasonCode, props))
          .left.map(BadDisconnectMessage)
      } catch {
        case _: NoSuchElementException => Left(BadDisconnectMessage(BufferUnderflow))
      }

    // 3.14.2.2 DISCONNECT Properties
    def decodeDisconnectProperties(): Either[DecodeError, DisconnectProperties] =
      try {
        val length = v.decodeVariableByteInteger()
        length.flatMap(l => {
          val propBytesArray = Array.fill(l.underlying)(0.toByte)
          v.copyToArray(propBytesArray)
          val propBytes = ByteArrayIterator(propBytesArray)
          @tailrec
          def iterate(props: DisconnectProperties): Either[DecodeError, DisconnectProperties] = {
            if (propBytes.isEmpty)
              Right(props)
            else {
              val propId = propBytes.decodeVariableByteInteger()
              propId match {
                case Right(PropertyIdentifiers.SessionExpiryInterval) =>
                  if (props.SessionExpiryInterval.isEmpty)
                    propBytes.decodeFourByteInteger() match {
                      case Right(value) =>
                        iterate(props.copy(SessionExpiryInterval = Some(value)))
                      case Left(e) => Left(e)
                    }
                  else
                    Left(SessionExpiryIntervalDefinedMoreThanOnce)
                case Right(PropertyIdentifiers.ReasonString) =>
                  if (props.ReasonString.isEmpty)
                    propBytes.decodeString() match {
                      case Right(value) =>
                        iterate(props.copy(ReasonString = Some(value)))
                      case Left(e) => Left(e)
                    }
                  else
                    Left(ReasonStringDefinedMoreThanOnce)
                case Right(PropertyIdentifiers.UserProperty) =>
                  propBytes.decodeStringPair() match {
                    case Right(userProp) =>
                      val currentUserProps = props.UserProperties
                      val newUserProps = currentUserProps.fold(List(userProp))(_ :+ userProp)
                      iterate(props.copy(UserProperties = Some(newUserProps)))
                    case Left(e) => Left(e)
                  }
                case Right(PropertyIdentifiers.ServerReference) =>
                  if (props.ServerReference.isEmpty)
                    propBytes.decodeString() match {
                      case Right(value) =>
                        iterate(props.copy(ServerReference = Some(value)))
                      case Left(e) => Left(e)
                    }
                  else
                    Left(ServerReferenceDefinedMoreThanOnce)
                case Right(id) =>
                  Left(UnknownPropertyId(id))
                case Left(e) => Left(e)
              }
            }
          }
          iterate(DisconnectProperties())
            .left.map(BadDisconnectProperties)
        })
      } catch {
        case _: NoSuchElementException => Left(BufferUnderflow)
      }

    // 3.15 AUTH - Authentication Exchange
    def decodeAuth(): Either[DecodeError, Auth] =
      try {
        val reasonCode = AuthReasonCode(v.getByte & 0xff)
        (for {
          props <- v.decodeAuthProperties()
        } yield Auth(reasonCode, props))
          .left.map(BadAuthMessage)
      } catch {
        case _: NoSuchElementException => Left(BadAuthMessage(BufferUnderflow))
      }

    // 3.15 AUTH Properties
    def decodeAuthProperties(): Either[DecodeError, AuthProperties] =
      try {
        val length = v.decodeVariableByteInteger()
        length.flatMap(l => {
          val propBytesArray = Array.fill(l.underlying)(0.toByte)
          v.copyToArray(propBytesArray)
          val propBytes = ByteArrayIterator(propBytesArray)
          @tailrec
          def iterate(props: AuthProperties): Either[DecodeError, AuthProperties] = {
            if (propBytes.isEmpty)
              Right(props)
            else {
              val propId = propBytes.decodeVariableByteInteger()
              propId match {
                case Right(PropertyIdentifiers.AuthenticationMethod) =>
                  if (props.AuthenticationMethod.isEmpty)
                    propBytes.decodeString() match {
                      case Right(str) =>
                        iterate(props.copy(AuthenticationMethod = str))
                      case Left(e) => Left(e)
                    }
                  else
                    Left(AuthenticationMethodDefinedMoreThanOnce)
                case Right(PropertyIdentifiers.AuthenticationData) =>
                  if (props.AuthenticationData.isEmpty)
                    propBytes.decodeBinaryData() match {
                      case Right(binaryData) =>
                        iterate(props.copy(AuthenticationData = Some(binaryData)))
                      case Left(e) => Left(e)
                    }
                  else
                    Left(AuthenticationDataDefinedMoreThanOnce)
                case Right(PropertyIdentifiers.ReasonString) =>
                  if (props.ReasonString.isEmpty)
                    propBytes.decodeString() match {
                      case Right(value) =>
                        iterate(props.copy(ReasonString = Some(value)))
                      case Left(e) => Left(e)
                    }
                  else
                    Left(ReasonStringDefinedMoreThanOnce)
                case Right(PropertyIdentifiers.UserProperty) =>
                  propBytes.decodeStringPair() match {
                    case Right(userProp) =>
                      val currentUserProps = props.UserProperties
                      val newUserProps = currentUserProps.fold(List(userProp))(_ :+ userProp)
                      iterate(props.copy(UserProperties = Some(newUserProps)))
                    case Left(e) => Left(e)
                  }
                case Right(id) =>
                  Left(UnknownPropertyId(id))
                case Left(e) => Left(e)
              }
            }
          }
          iterate(AuthProperties()) match {
            case Right(authProperties) =>
              if (authProperties.AuthenticationMethod.isEmpty)
                Left(AuthDataDefinedWhileAuthMethodUndefined)
              else
                Right(authProperties)
            case Left(err) => Left(BadConnectProperties(err))
          }
        })
      } catch {
        case _: NoSuchElementException => Left(BufferUnderflow)
      }

  }
}


object Command {
  def apply[A](command: ControlPacket): Command[A] =
    new Command(command)

  def apply[A](command: ControlPacket, carry: A): Command[A] =
    new Command(command, carry)
}

final case class Command[A](command: ControlPacket, completed: Option[Promise[Done]], carry: Option[A]) {
  def this(command: ControlPacket) =
    this(command, None, None)

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
