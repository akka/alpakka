/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.mqtt.streaming.scaladsl

import akka.NotUsed
import akka.stream.alpakka.mqtt.streaming._
import akka.stream.alpakka.mqtt.streaming.impl.MqttFrameStage
import akka.stream.scaladsl.Flow
import akka.util.ByteString

object MqttSession {

  private[streaming] type CommandFlow =
    Flow[Command[_], ByteString, NotUsed]
  private[streaming] type EventFlow =
    Flow[ByteString, Either[MqttCodec.DecodeError, Event[_]], NotUsed]
}

/**
 * Represents MQTT session state for both clients or servers. Session
 * state can survive across connections i.e. their lifetime is
 * generally longer.
 */
abstract class MqttSession {

  import MqttSession._

  /**
   * @return a flow for commands to be sent to the session
   */
  private[streaming] def commandFlow: CommandFlow

  /**
   * @return a flow for events to be emitted by the session
   */
  private[streaming] def eventFlow: EventFlow

}

/**
 * Represents client-only sessions
 */
abstract class MqttClientSession extends MqttSession

object ActorMqttClientSession {
  def apply(settings: MqttSessionSettings): ActorMqttClientSession =
    new ActorMqttClientSession(settings)
}

/**
 * Provides an actor implementation of a client session
 * @param settings session settings
 */
final class ActorMqttClientSession(settings: MqttSessionSettings) extends MqttClientSession {
  //private val actor: ActorRef = ???

  import MqttCodec._
  import MqttSession._

  override def commandFlow: CommandFlow =
    Flow[Command[_]].map {
      case Command(cp: Connect, _) => cp.encode(ByteString.newBuilder).result()
      case Command(cp: Publish, _) => cp.encode(ByteString.newBuilder, cp.packetId).result()
      case Command(cp: PubRec, _) => cp.encode(ByteString.newBuilder).result()
      case Command(cp: PubRel, _) => cp.encode(ByteString.newBuilder).result()
      case Command(cp: PubComp, _) => cp.encode(ByteString.newBuilder).result()
      case Command(cp: Subscribe, _) => cp.encode(ByteString.newBuilder, cp.packetId).result()
      case Command(cp: Unsubscribe, _) => cp.encode(ByteString.newBuilder, cp.packetId).result()
      case Command(cp: PingReq.type, _) => cp.encode(ByteString.newBuilder).result()
      case Command(cp: Disconnect.type, _) => cp.encode(ByteString.newBuilder).result()
      case c: Command[_] => throw new IllegalStateException(c + " is not a client command")
    }

  override def eventFlow: EventFlow =
    Flow[ByteString]
      .via(new MqttFrameStage(settings.maxPacketSize))
      .map(_.iterator.decodeControlPacket(settings.maxPacketSize).map(Event(_, None)))

}

/**
 * Represents server-only sessions
 */
abstract class MqttServerSession extends MqttSession

object ActorMqttServerSession {
  def apply(settings: MqttSessionSettings): ActorMqttServerSession =
    new ActorMqttServerSession(settings)
}

/**
 * Provides an actor implementation of a server session
 * @param settings session settings
 */
final class ActorMqttServerSession(settings: MqttSessionSettings) extends MqttServerSession {
  //private val actor: ActorRef = ???

  import MqttCodec._
  import MqttSession._

  override def commandFlow: CommandFlow =
    Flow[Command[_]].map {
      case Command(cp: ConnAck, _) => cp.encode(ByteString.newBuilder).result()
      case Command(cp: Publish, _) => cp.encode(ByteString.newBuilder, cp.packetId).result()
      case Command(cp: PubAck, _) => cp.encode(ByteString.newBuilder).result()
      case Command(cp: PubRec, _) => cp.encode(ByteString.newBuilder).result()
      case Command(cp: PubRel, _) => cp.encode(ByteString.newBuilder).result()
      case Command(cp: PubComp, _) => cp.encode(ByteString.newBuilder).result()
      case Command(cp: SubAck, _) => cp.encode(ByteString.newBuilder).result()
      case Command(cp: UnsubAck, _) => cp.encode(ByteString.newBuilder).result()
      case Command(cp: PingResp.type, _) => cp.encode(ByteString.newBuilder).result()
      case c: Command[_] => throw new IllegalStateException(c + " is not a server command")
    }

  override def eventFlow: EventFlow =
    Flow[ByteString]
      .via(new MqttFrameStage(settings.maxPacketSize))
      .map(_.iterator.decodeControlPacket(settings.maxPacketSize).map(Event(_, None)))
}
