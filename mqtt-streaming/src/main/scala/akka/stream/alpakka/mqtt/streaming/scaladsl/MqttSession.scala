/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.mqtt.streaming
package scaladsl

import akka.actor.Scheduler
import akka.{Done, NotUsed, actor => untyped}
import akka.actor.typed.scaladsl.adapter._
import akka.actor.typed.scaladsl.AskPattern._
import akka.stream.alpakka.mqtt.streaming.impl.{ClientConnector, MqttFrameStage}
import akka.stream.scaladsl.Flow
import akka.util.{ByteString, Timeout}

import scala.concurrent.Future

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
   * Disconnect the session gracefully
   * @return [[Done]] when complete
   */
  def disconnect(): Future[Done]

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
  def apply(settings: MqttSessionSettings)(implicit system: untyped.ActorSystem): ActorMqttClientSession =
    new ActorMqttClientSession(settings)
}

/**
 * Provides an actor implementation of a client session
 * @param settings session settings
 */
final class ActorMqttClientSession(settings: MqttSessionSettings)(implicit system: untyped.ActorSystem)
    extends MqttClientSession {

  private val clientConnector = system.spawn(ClientConnector(settings), "client-connector")

  import MqttCodec._
  import MqttSession._

  implicit private val actorMqttSessionTimeout: Timeout = settings.actorMqttSessionTimeout
  implicit private val scheduler: Scheduler = system.scheduler

  import system.dispatcher

  override def disconnect(): Future[Done] = ???

  override def commandFlow: CommandFlow =
    Flow[Command[_]]
      .mapAsync(1) {
        case Command(cp: Connect, carry) =>
          (clientConnector ? (replyTo => ClientConnector.ConnectReceivedLocally(cp, carry, replyTo)): Future[
            ClientConnector.ForwardConnect.type
          ]).map(_ => cp.encode(ByteString.newBuilder).result())
        // TODO: Forward the following messages on as per the above ask pattern
        case Command(cp: Publish, _) => Future.successful(cp.encode(ByteString.newBuilder, cp.packetId).result())
        case Command(cp: PubRec, _) => Future.successful(cp.encode(ByteString.newBuilder).result())
        case Command(cp: PubRel, _) => Future.successful(cp.encode(ByteString.newBuilder).result())
        case Command(cp: PubComp, _) => Future.successful(cp.encode(ByteString.newBuilder).result())
        case Command(cp: Subscribe, _) => Future.successful(cp.encode(ByteString.newBuilder, cp.packetId).result())
        case Command(cp: Unsubscribe, _) => Future.successful(cp.encode(ByteString.newBuilder, cp.packetId).result())
        case Command(cp: PingReq.type, _) => Future.successful(cp.encode(ByteString.newBuilder).result())
        case Command(cp: Disconnect.type, carry) =>
          (clientConnector ? (replyTo => ClientConnector.DisconnectReceivedLocally(replyTo)): Future[
            ClientConnector.ForwardDisconnect.type
          ]).map(_ => cp.encode(ByteString.newBuilder).result())
        case c: Command[_] => throw new IllegalStateException(c + " is not a client command")
      }

  override def eventFlow: EventFlow =
    Flow[ByteString]
      .via(new MqttFrameStage(settings.maxPacketSize))
      .map(_.iterator.decodeControlPacket(settings.maxPacketSize))
      .mapAsync(1) {
        case Right(connAck: ConnAck) =>
          import system.dispatcher
          (clientConnector ? (ClientConnector
            .ConnAckReceivedFromRemote(connAck, _)): Future[ClientConnector.ForwardConnAck])
            .map {
              case ClientConnector.ForwardConnAck(carry) => Right[DecodeError, Event[_]](Event(connAck, carry))
            }
        // TODO: Forward the following messages on as per the above ask pattern
        case Right(cp) => Future.successful(Right[DecodeError, Event[_]](Event(cp)))
        case Left(de) => Future.successful(Left(de))
      }

}

/**
 * Represents server-only sessions
 */
abstract class MqttServerSession extends MqttSession

object ActorMqttServerSession {
  def apply(settings: MqttSessionSettings)(implicit system: untyped.ActorSystem): ActorMqttServerSession =
    new ActorMqttServerSession(settings)
}

/**
 * Provides an actor implementation of a server session
 * @param settings session settings
 */
final class ActorMqttServerSession(settings: MqttSessionSettings)(implicit system: untyped.ActorSystem)
    extends MqttServerSession {

  //private val actor: ActorRef = ???

  import MqttCodec._
  import MqttSession._

  override def disconnect(): Future[Done] = ???

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
