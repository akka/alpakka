/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.mqtt.streaming
package scaladsl

import java.util.concurrent.atomic.AtomicLong

import akka.actor.Scheduler
import akka.{Done, NotUsed, actor => untyped}
import akka.actor.typed.scaladsl.adapter._
import akka.actor.typed.scaladsl.AskPattern._
import akka.stream.alpakka.mqtt.streaming.impl._
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
   * Shutdown the session gracefully
   * @return [[Done]] when complete
   */
  def shutdown(): Future[Done]

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

  private[scaladsl] val clientSessionCounter = new AtomicLong
}

/**
 * Provides an actor implementation of a client session
 * @param settings session settings
 */
final class ActorMqttClientSession(settings: MqttSessionSettings)(implicit system: untyped.ActorSystem)
    extends MqttClientSession {

  import ActorMqttClientSession._

  private val clientSessionId = clientSessionCounter.getAndIncrement()
  private val producerPacketRouter =
    system.spawn(PacketRouter[Producer.Event], "client-producer-packet-id-allocator-" + clientSessionId)
  private val subscriberPacketRouter =
    system.spawn(PacketRouter[Subscriber.Event], "client-subscriber-packet-id-allocator-" + clientSessionId)
  private val clientConnector =
    system.spawn(ClientConnector(producerPacketRouter, subscriberPacketRouter, settings),
                 "client-connector-" + clientSessionId)

  import MqttCodec._
  import MqttSession._

  implicit private val actorMqttSessionTimeout: Timeout = settings.actorMqttSessionTimeout
  implicit private val scheduler: Scheduler = system.scheduler

  import system.dispatcher

  override def shutdown(): Future[Done] = ???

  override def commandFlow: CommandFlow =
    Flow[Command[_]]
      .mapAsync(1) {
        case Command(cp: Connect, carry) =>
          (clientConnector ? (replyTo => ClientConnector.ConnectReceivedLocally(cp, carry, replyTo)): Future[
            ClientConnector.ForwardConnect.type
          ]).map(_ => cp.encode(ByteString.newBuilder).result())
        case Command(cp: Publish, carry) =>
          (clientConnector ? (replyTo => ClientConnector.PublishReceivedLocally(cp, carry, replyTo)): Future[
            Producer.ForwardPublish
          ]).map(command => cp.encode(ByteString.newBuilder, command.packetId).result())
        case Command(cp: PubAck, _) =>
          (subscriberPacketRouter ? (
              replyTo => PacketRouter.Route(cp.packetId, Subscriber.PubAckReceivedLocally(replyTo))
          ): Future[
            Subscriber.ForwardPubAck.type
          ]).map(_ => cp.encode(ByteString.newBuilder).result())
        case Command(cp: PubRec, _) =>
          (subscriberPacketRouter ? (
              replyTo => PacketRouter.Route(cp.packetId, Subscriber.PubRecReceivedLocally(replyTo))
          ): Future[
            Subscriber.ForwardPubRec.type
          ]).map(_ => cp.encode(ByteString.newBuilder).result())
        case Command(cp: PubComp, _) =>
          (subscriberPacketRouter ? (
              replyTo => PacketRouter.Route(cp.packetId, Subscriber.PubCompReceivedLocally(replyTo))
          ): Future[
            Subscriber.ForwardPubComp.type
          ]).map(_ => cp.encode(ByteString.newBuilder).result())
        case Command(cp: Subscribe, carry) =>
          (clientConnector ? (replyTo => ClientConnector.SubscribeReceivedLocally(cp, carry, replyTo)): Future[
            Subscriber.ForwardSubscribe
          ]).map(command => cp.encode(ByteString.newBuilder, command.packetId).result())
        case Command(cp: Disconnect.type, _) =>
          (clientConnector ? (replyTo => ClientConnector.DisconnectReceivedLocally(replyTo)): Future[
            ClientConnector.ForwardDisconnect.type
          ]).map(_ => cp.encode(ByteString.newBuilder).result())
        case c: Command[_] => throw new IllegalStateException(c + " is not a client command")
      }
      .watchTermination() {
        case (_, terminated) =>
          terminated.foreach(_ => clientConnector ! ClientConnector.ConnectionLost)
          NotUsed
      }

  override def eventFlow: EventFlow =
    Flow[ByteString]
      .via(new MqttFrameStage(settings.maxPacketSize))
      .map(_.iterator.decodeControlPacket(settings.maxPacketSize))
      .mapAsync(1) {
        case Right(cp: ConnAck) =>
          (clientConnector ? (ClientConnector
            .ConnAckReceivedFromRemote(cp, _)): Future[ClientConnector.ForwardConnAck])
            .map {
              case ClientConnector.ForwardConnAck(carry) => Right[DecodeError, Event[_]](Event(cp, carry))
            }
        case Right(cp: SubAck) =>
          (subscriberPacketRouter ? (
              replyTo =>
                PacketRouter.Route(cp.packetId,
                                   Subscriber
                                     .SubAckReceivedFromRemote(replyTo))
          ): Future[Subscriber.ForwardSubAck])
            .map {
              case Subscriber.ForwardSubAck(carry) => Right[DecodeError, Event[_]](Event(cp, carry))
            }
        case Right(cp: Publish) =>
          (clientConnector ? (ClientConnector
            .PublishReceivedFromRemote(cp, _)): Future[Subscriber.ForwardPublish.type])
            .map {
              case Subscriber.ForwardPublish => Right[DecodeError, Event[_]](Event(cp))
            }
        case Right(cp: PubAck) =>
          (producerPacketRouter ? (
              replyTo =>
                PacketRouter.Route(cp.packetId,
                                   Producer
                                     .PubAckReceivedFromRemote(replyTo))
          ): Future[Producer.ForwardPubAck])
            .map {
              case Producer.ForwardPubAck(carry) => Right[DecodeError, Event[_]](Event(cp, carry))
            }
        case Right(cp: PubRel) =>
          (subscriberPacketRouter ? (
              replyTo =>
                PacketRouter.Route(cp.packetId,
                                   Subscriber
                                     .PubRelReceivedFromRemote(replyTo))
          ): Future[Subscriber.ForwardPubRel.type])
            .map {
              case Subscriber.ForwardPubRel => Right[DecodeError, Event[_]](Event(cp))
            }
        case Right(cp) => Future.failed(new IllegalStateException(cp + " is not a client event"))
        case Left(de) => Future.successful(Left(de))
      }
      .watchTermination() {
        case (_, terminated) =>
          terminated.foreach(_ => clientConnector ! ClientConnector.ConnectionLost)
          NotUsed
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

  override def shutdown(): Future[Done] = ???

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
