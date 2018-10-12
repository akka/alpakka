/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.mqtt.streaming
package scaladsl

import java.util.concurrent.atomic.AtomicLong

import akka.actor.Scheduler
import akka.{NotUsed, actor => untyped}
import akka.actor.typed.scaladsl.adapter._
import akka.actor.typed.scaladsl.AskPattern._
import akka.stream.alpakka.mqtt.streaming.impl._
import akka.stream.scaladsl.{Flow, Source}
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

  /**
   * Shutdown the session gracefully
   */
  def shutdown(): Unit
}

/**
 * Represents client-only sessions
 */
abstract class MqttClientSession extends MqttSession {
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

object ActorMqttClientSession {
  def apply(settings: MqttSessionSettings)(implicit system: untyped.ActorSystem): ActorMqttClientSession =
    new ActorMqttClientSession(settings)

  /**
   * A PINGREQ failed to receive a PINGRESP - the connection must close
   *
   * http://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html
   * 3.1.2.10 Keep Alive
   */
  case object PingFailed extends Exception

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
  private val consumerPacketRouter =
    system.spawn(RemotePacketRouter[Consumer.Event], "client-consumer-packet-id-allocator-" + clientSessionId)
  private val producerPacketRouter =
    system.spawn(LocalPacketRouter[Producer.Event], "client-producer-packet-id-allocator-" + clientSessionId)
  private val subscriberPacketRouter =
    system.spawn(LocalPacketRouter[Subscriber.Event], "client-subscriber-packet-id-allocator-" + clientSessionId)
  private val unsubscriberPacketRouter =
    system.spawn(LocalPacketRouter[Unsubscriber.Event], "client-unsubscriber-packet-id-allocator-" + clientSessionId)
  private val clientConnector =
    system.spawn(
      ClientConnector(consumerPacketRouter,
                      producerPacketRouter,
                      subscriberPacketRouter,
                      unsubscriberPacketRouter,
                      settings),
      "client-connector-" + clientSessionId
    )

  import MqttCodec._
  import MqttSession._

  implicit private val actorMqttSessionTimeout: Timeout = settings.actorMqttSessionTimeout
  implicit private val scheduler: Scheduler = system.scheduler

  import system.dispatcher

  override def shutdown(): Unit = {
    system.stop(clientConnector.toUntyped)
    system.stop(consumerPacketRouter.toUntyped)
    system.stop(producerPacketRouter.toUntyped)
    system.stop(subscriberPacketRouter.toUntyped)
    system.stop(unsubscriberPacketRouter.toUntyped)
  }

  private val pingReqBytes = PingReq.encode(ByteString.newBuilder).result()

  override def commandFlow: CommandFlow =
    Flow[Command[_]]
      .watch(clientConnector.toUntyped)
      .watchTermination() {
        case (_, terminated) =>
          terminated.foreach(_ => clientConnector ! ClientConnector.ConnectionLost)
          NotUsed
      }
      .flatMapMerge(
        settings.commandParallelism, {
          case Command(cp: Connect, carry) =>
            Source.fromFutureSource(
              (clientConnector ? (replyTo => ClientConnector.ConnectReceivedLocally(cp, carry, replyTo)): Future[
                Source[ClientConnector.ForwardConnectCommand, NotUsed]
              ]).map(_.map {
                case ClientConnector.ForwardConnect => cp.encode(ByteString.newBuilder).result()
                case ClientConnector.ForwardPingReq => pingReqBytes
              }.mapError {
                case ClientConnector.PingFailed => ActorMqttClientSession.PingFailed
              })
            )
          case Command(cp: Publish, carry) =>
            Source.fromFutureSource(
              (clientConnector ? (replyTo => ClientConnector.PublishReceivedLocally(cp, carry, replyTo)): Future[
                Source[Producer.ForwardPublishingCommand, NotUsed]
              ]).map(_.map {
                case Producer.ForwardPublish(packetId, false) =>
                  cp.encode(ByteString.newBuilder, packetId).result()
                case Producer.ForwardPublish(packetId, true) =>
                  cp.copy(flags = cp.flags | ControlPacketFlags.DUP).encode(ByteString.newBuilder, packetId).result()
                case Producer.ForwardPubRel(packetId) =>
                  PubRel(packetId).encode(ByteString.newBuilder).result()
              })
            )
          case Command(cp: PubAck, _) =>
            Source.fromFuture(
              (consumerPacketRouter ? (
                  replyTo => RemotePacketRouter.Route(cp.packetId, Consumer.PubAckReceivedLocally(replyTo))
              ): Future[
                Consumer.ForwardPubAck.type
              ]).map(_ => cp.encode(ByteString.newBuilder).result())
            )
          case Command(cp: PubRec, _) =>
            Source.fromFuture(
              (consumerPacketRouter ? (
                  replyTo => RemotePacketRouter.Route(cp.packetId, Consumer.PubRecReceivedLocally(replyTo))
              ): Future[
                Consumer.ForwardPubRec.type
              ]).map(_ => cp.encode(ByteString.newBuilder).result())
            )
          case Command(cp: PubComp, _) =>
            Source.fromFuture(
              (consumerPacketRouter ? (
                  replyTo => RemotePacketRouter.Route(cp.packetId, Consumer.PubCompReceivedLocally(replyTo))
              ): Future[
                Consumer.ForwardPubComp.type
              ]).map(_ => cp.encode(ByteString.newBuilder).result())
            )
          case Command(cp: Subscribe, carry) =>
            Source.fromFuture(
              (clientConnector ? (replyTo => ClientConnector.SubscribeReceivedLocally(cp, carry, replyTo)): Future[
                Subscriber.ForwardSubscribe
              ]).map(command => cp.encode(ByteString.newBuilder, command.packetId).result())
            )
          case Command(cp: Unsubscribe, carry) =>
            Source.fromFuture(
              (clientConnector ? (replyTo => ClientConnector.UnsubscribeReceivedLocally(cp, carry, replyTo)): Future[
                Unsubscriber.ForwardUnsubscribe
              ]).map(command => cp.encode(ByteString.newBuilder, command.packetId).result())
            )
          case Command(cp: Disconnect.type, _) =>
            Source.fromFuture(
              (clientConnector ? (replyTo => ClientConnector.DisconnectReceivedLocally(replyTo)): Future[
                ClientConnector.ForwardDisconnect.type
              ]).map(_ => cp.encode(ByteString.newBuilder).result())
            )
          case c: Command[_] => throw new IllegalStateException(c + " is not a client command")
        }
      )

  override def eventFlow: EventFlow =
    Flow[ByteString]
      .watch(clientConnector.toUntyped)
      .watchTermination() {
        case (_, terminated) =>
          terminated.foreach(_ => clientConnector ! ClientConnector.ConnectionLost)
          NotUsed
      }
      .via(new MqttFrameStage(settings.maxPacketSize))
      .map(_.iterator.decodeControlPacket(settings.maxPacketSize))
      .mapAsync(settings.eventParallelism) {
        case Right(cp: ConnAck) =>
          (clientConnector ? (ClientConnector
            .ConnAckReceivedFromRemote(cp, _)): Future[ClientConnector.ForwardConnAck])
            .map {
              case ClientConnector.ForwardConnAck(carry) => Right[DecodeError, Event[_]](Event(cp, carry))
            }
        case Right(cp: SubAck) =>
          (subscriberPacketRouter ? (
              replyTo =>
                LocalPacketRouter.Route(cp.packetId,
                                        Subscriber
                                          .SubAckReceivedFromRemote(replyTo))
          ): Future[Subscriber.ForwardSubAck])
            .map {
              case Subscriber.ForwardSubAck(carry) => Right[DecodeError, Event[_]](Event(cp, carry))
            }
        case Right(cp: UnsubAck) =>
          (unsubscriberPacketRouter ? (
              replyTo =>
                LocalPacketRouter.Route(cp.packetId,
                                        Unsubscriber
                                          .UnsubAckReceivedFromRemote(replyTo))
          ): Future[Unsubscriber.ForwardUnsubAck])
            .map {
              case Unsubscriber.ForwardUnsubAck(carry) => Right[DecodeError, Event[_]](Event(cp, carry))
            }
        case Right(cp: Publish) =>
          (clientConnector ? (ClientConnector
            .PublishReceivedFromRemote(cp, _)): Future[Consumer.ForwardPublish.type])
            .map(_ => Right[DecodeError, Event[_]](Event(cp)))
        case Right(cp: PubAck) =>
          (producerPacketRouter ? (
              replyTo =>
                LocalPacketRouter.Route(cp.packetId,
                                        Producer
                                          .PubAckReceivedFromRemote(replyTo))
          ): Future[Producer.ForwardPubAck])
            .map {
              case Producer.ForwardPubAck(carry) => Right[DecodeError, Event[_]](Event(cp, carry))
            }
        case Right(cp: PubRec) =>
          (producerPacketRouter ? (
              replyTo =>
                LocalPacketRouter.Route(cp.packetId,
                                        Producer
                                          .PubRecReceivedFromRemote(replyTo))
          ): Future[Producer.ForwardPubRec])
            .map {
              case Producer.ForwardPubRec(carry) => Right[DecodeError, Event[_]](Event(cp, carry))
            }
        case Right(cp: PubRel) =>
          (consumerPacketRouter ? (
              replyTo =>
                RemotePacketRouter.Route(cp.packetId,
                                         Consumer
                                           .PubRelReceivedFromRemote(replyTo))
          ): Future[Consumer.ForwardPubRel.type]).map(_ => Right[DecodeError, Event[_]](Event(cp)))
        case Right(cp: PubComp) =>
          (producerPacketRouter ? (
              replyTo =>
                LocalPacketRouter.Route(cp.packetId,
                                        Producer
                                          .PubCompReceivedFromRemote(replyTo))
          ): Future[Producer.ForwardPubComp])
            .map {
              case Producer.ForwardPubComp(carry) => Right[DecodeError, Event[_]](Event(cp, carry))
            }
        case Right(PingResp) =>
          (clientConnector ? ClientConnector.PingRespReceivedFromRemote: Future[ClientConnector.ForwardPingResp.type])
            .map(_ => Right[DecodeError, Event[_]](Event(PingResp)))
        case Right(cp) => Future.failed(new IllegalStateException(cp + " is not a client event"))
        case Left(de) => Future.successful(Left(de))
      }
}

/**
 * Represents server-only sessions
 */
abstract class MqttServerSession extends MqttSession {
  import MqttSession._

  /**
   * @return a flow for commands to be sent to the session in relation to a connection id
   */
  private[streaming] def commandFlow(connectionId: ByteString): CommandFlow

  /**
   * @return a flow for events to be emitted by the session in relation t a connection id
   */
  private[streaming] def eventFlow(connectionId: ByteString): EventFlow
}

object ActorMqttServerSession {
  def apply(settings: MqttSessionSettings)(implicit system: untyped.ActorSystem): ActorMqttServerSession =
    new ActorMqttServerSession(settings)

  /**
   * A PINGREQ was not received within the required keep alive period - the connection must close
   *
   * http://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html
   * 3.1.2.10 Keep Alive
   */
  case object PingFailed extends Exception

  private[scaladsl] val serverSessionCounter = new AtomicLong
}

/**
 * Provides an actor implementation of a server session
 * @param settings session settings
 */
final class ActorMqttServerSession(settings: MqttSessionSettings)(implicit system: untyped.ActorSystem)
    extends MqttServerSession {

  import ActorMqttServerSession._

  private val serverSessionId = serverSessionCounter.getAndIncrement()

  private val consumerPacketRouter =
    system.spawn(RemotePacketRouter[Consumer.Event], "server-consumer-packet-id-allocator-" + serverSessionId)
  private val producerPacketRouter =
    system.spawn(LocalPacketRouter[Producer.Event], "server-producer-packet-id-allocator-" + serverSessionId)
  private val publisherPacketRouter =
    system.spawn(RemotePacketRouter[Publisher.Event], "server-publisher-packet-id-allocator-" + serverSessionId)
  private val unpublisherPacketRouter =
    system.spawn(RemotePacketRouter[Unpublisher.Event], "server-unpublisher-packet-id-allocator-" + serverSessionId)
  private val serverConnector =
    system.spawn(
      ServerConnector(consumerPacketRouter,
                      producerPacketRouter,
                      publisherPacketRouter,
                      unpublisherPacketRouter,
                      settings),
      "server-connector-" + serverSessionId
    )

  import MqttCodec._
  import MqttSession._

  implicit private val actorMqttSessionTimeout: Timeout = settings.actorMqttSessionTimeout
  implicit private val scheduler: Scheduler = system.scheduler

  import system.dispatcher

  override def shutdown(): Unit = {
    system.stop(serverConnector.toUntyped)
    system.stop(consumerPacketRouter.toUntyped)
    system.stop(producerPacketRouter.toUntyped)
    system.stop(publisherPacketRouter.toUntyped)
    system.stop(unpublisherPacketRouter.toUntyped)
  }

  private val pingRespBytes = PingResp.encode(ByteString.newBuilder).result()

  override def commandFlow(connectionId: ByteString): CommandFlow =
    Flow[Command[_]]
      .watch(serverConnector.toUntyped)
      .watchTermination() {
        case (_, terminated) =>
          terminated.foreach(_ => serverConnector ! ServerConnector.ConnectionLost(connectionId))
          NotUsed
      }
      .flatMapMerge(
        settings.commandParallelism, {
          case Command(cp: ConnAck, _) =>
            Source.fromFutureSource(
              (serverConnector ? (replyTo => ServerConnector.ConnAckReceivedLocally(connectionId, cp, replyTo)): Future[
                Source[ClientConnection.ForwardConnAckCommand, NotUsed]
              ]).map(_.map {
                case ClientConnection.ForwardConnAck => cp.encode(ByteString.newBuilder).result()
              }.mapError {
                case ServerConnector.PingFailed => ActorMqttServerSession.PingFailed
              })
            )
          case Command(cp: SubAck, _) =>
            Source.fromFuture(
              (publisherPacketRouter ? (
                  replyTo => RemotePacketRouter.Route(cp.packetId, Publisher.SubAckReceivedLocally(replyTo))
              ): Future[Publisher.ForwardSubAck.type]).map(_ => cp.encode(ByteString.newBuilder).result())
            )
          case Command(cp: UnsubAck, _) =>
            Source.fromFuture(
              (unpublisherPacketRouter ? (
                  replyTo => RemotePacketRouter.Route(cp.packetId, Unpublisher.UnsubAckReceivedLocally(replyTo))
              ): Future[Unpublisher.ForwardUnsubAck.type]).map(_ => cp.encode(ByteString.newBuilder).result())
            )
          case c: Command[_] => throw new IllegalStateException(c + " is not a server command")
        }
      )

  override def eventFlow(connectionId: ByteString): EventFlow =
    Flow[ByteString]
      .watch(serverConnector.toUntyped)
      .watchTermination() {
        case (_, terminated) =>
          terminated.foreach(_ => serverConnector ! ServerConnector.ConnectionLost(connectionId))
          NotUsed
      }
      .via(new MqttFrameStage(settings.maxPacketSize))
      .map(_.iterator.decodeControlPacket(settings.maxPacketSize))
      .mapAsync(settings.eventParallelism) {
        case Right(cp: Connect) =>
          (serverConnector ? (ServerConnector
            .ConnectReceivedFromRemote(connectionId, cp, _)): Future[ClientConnection.ForwardConnect.type])
            .map(_ => Right[DecodeError, Event[_]](Event(cp)))
        case Right(cp: Subscribe) =>
          (serverConnector ? (ServerConnector
            .SubscribeReceivedFromRemote(connectionId, cp, _)): Future[Publisher.ForwardSubscribe.type])
            .map(_ => Right[DecodeError, Event[_]](Event(cp)))
        case Right(cp: Unsubscribe) =>
          (serverConnector ? (ServerConnector
            .UnsubscribeReceivedFromRemote(connectionId, cp, _)): Future[Unpublisher.ForwardUnsubscribe.type])
            .map(_ => Right[DecodeError, Event[_]](Event(cp)))
        case Right(cp) => Future.failed(new IllegalStateException(cp + " is not a server event"))
        case Left(de) => Future.successful(Left(de))
      }
}
