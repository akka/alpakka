/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.mqtt.streaming
package impl

import java.nio.charset.StandardCharsets

import akka.NotUsed
import akka.actor.typed.{ActorRef, Behavior, PostStop}
import akka.actor.typed.scaladsl.Behaviors
import akka.annotation.InternalApi
import akka.stream.{Materializer, OverflowStrategy}
import akka.stream.scaladsl.{BroadcastHub, Keep, Source, SourceQueueWithComplete}
import akka.stream.typed.scaladsl.ActorMaterializer
import akka.util.Timeout

import scala.concurrent.duration.FiniteDuration
import scala.util.{Failure, Success}

/*
 * A client connector is a Finite State Machine that manages MQTT client
 * session state. A client connects to a server, subscribes/unsubscribes
 * from topics to receive publications on and publishes to its own topics.
 */
@InternalApi private[streaming] object ClientConnector {

  type ConnectData = Option[_]

  /*
   * A PINGREQ failed to receive a PINGRESP - the connection must close
   *
   * http://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html
   * 3.1.2.10 Keep Alive
   */
  case object PingFailed extends Exception

  /*
   * Construct with the starting state
   */
  def apply(consumerPacketRouter: ActorRef[RemotePacketRouter.Request[Consumer.Event]],
            producerPacketRouter: ActorRef[LocalPacketRouter.Request[Producer.Event]],
            subscriberPacketRouter: ActorRef[LocalPacketRouter.Request[Subscriber.Event]],
            settings: MqttSessionSettings): Behavior[Event] =
    disconnected(Uninitialized(consumerPacketRouter, producerPacketRouter, subscriberPacketRouter, settings))

  // Our FSM data, FSM events and commands emitted by the FSM

  sealed abstract class Data(val consumerPacketRouter: ActorRef[RemotePacketRouter.Request[Consumer.Event]],
                             val producerPacketRouter: ActorRef[LocalPacketRouter.Request[Producer.Event]],
                             val subscriberPacketRouter: ActorRef[LocalPacketRouter.Request[Subscriber.Event]],
                             val settings: MqttSessionSettings)
  final case class Uninitialized(
      override val consumerPacketRouter: ActorRef[RemotePacketRouter.Request[Consumer.Event]],
      override val producerPacketRouter: ActorRef[LocalPacketRouter.Request[Producer.Event]],
      override val subscriberPacketRouter: ActorRef[LocalPacketRouter.Request[Subscriber.Event]],
      override val settings: MqttSessionSettings
  ) extends Data(consumerPacketRouter, producerPacketRouter, subscriberPacketRouter, settings)
  final case class ConnectReceived(
      connect: Connect,
      connectData: ConnectData,
      stash: Seq[Event],
      remote: SourceQueueWithComplete[ForwardConnectCommand],
      override val consumerPacketRouter: ActorRef[RemotePacketRouter.Request[Consumer.Event]],
      override val producerPacketRouter: ActorRef[LocalPacketRouter.Request[Producer.Event]],
      override val subscriberPacketRouter: ActorRef[LocalPacketRouter.Request[Subscriber.Event]],
      override val settings: MqttSessionSettings
  ) extends Data(consumerPacketRouter, producerPacketRouter, subscriberPacketRouter, settings)
  final case class ConnAckReceived(
      keepAlive: FiniteDuration,
      pendingPingResp: Boolean,
      pendingLocalPublications: Seq[(String, PublishReceivedLocally)],
      remote: SourceQueueWithComplete[ForwardConnectCommand],
      override val consumerPacketRouter: ActorRef[RemotePacketRouter.Request[Consumer.Event]],
      override val producerPacketRouter: ActorRef[LocalPacketRouter.Request[Producer.Event]],
      override val subscriberPacketRouter: ActorRef[LocalPacketRouter.Request[Subscriber.Event]],
      override val settings: MqttSessionSettings
  ) extends Data(consumerPacketRouter, producerPacketRouter, subscriberPacketRouter, settings)

  sealed abstract class Event
  final case class ConnectReceivedLocally(connect: Connect,
                                          connectData: ConnectData,
                                          remote: ActorRef[Source[ForwardConnectCommand, NotUsed]])
      extends Event
  final case class ConnAckReceivedFromRemote(connAck: ConnAck, local: ActorRef[ForwardConnAck]) extends Event
  case object ReceiveConnAckTimeout extends Event
  case object ConnectionLost extends Event
  final case class DisconnectReceivedLocally(remote: ActorRef[ForwardDisconnect.type]) extends Event
  final case class SubscribeReceivedLocally(subscribe: Subscribe,
                                            subscribeData: Subscriber.SubscribeData,
                                            remote: ActorRef[Subscriber.ForwardSubscribe])
      extends Event
  final case class PublishReceivedFromRemote(publish: Publish, local: ActorRef[Consumer.ForwardPublish.type])
      extends Event
  final case class PublishReceivedLocally(publish: Publish,
                                          publishData: Producer.PublishData,
                                          remote: ActorRef[Source[Producer.ForwardPublishingCommand, NotUsed]])
      extends Event
  final case class ProducerFree(topicName: String) extends Event
  case object SendPingReqTimeout extends Event
  final case class PingRespReceivedFromRemote(local: ActorRef[ForwardPingResp.type]) extends Event

  sealed abstract class Command
  sealed abstract class ForwardConnectCommand
  case object ForwardConnect extends ForwardConnectCommand
  case object ForwardPingReq extends ForwardConnectCommand
  final case class ForwardConnAck(connectData: ConnectData) extends Command
  case object ForwardDisconnect extends Command
  case object ForwardPingResp extends Command

  // State event handling

  private val ConsumerNamePrefix = "consumer-"
  private val ProducerNamePrefix = "producer-"
  private val SubscriberNamePrefix = "subscriber-"

  private def mkActorName(name: String): String =
    name.getBytes(StandardCharsets.UTF_8).map(_.toHexString).mkString

  def disconnected(data: Uninitialized): Behavior[Event] = Behaviors.receivePartial {
    case (context, ConnectReceivedLocally(connect, connectData, remote)) =>
      implicit val mat: Materializer = ActorMaterializer()(context.system)
      val (queue, source) = Source
        .queue[ForwardConnectCommand](1, OverflowStrategy.dropHead)
        .toMat(BroadcastHub.sink)(Keep.both)
        .run()
      remote ! source

      queue.offer(ForwardConnect)
      serverConnect(
        ConnectReceived(connect,
                        connectData,
                        Vector.empty,
                        queue,
                        data.consumerPacketRouter,
                        data.producerPacketRouter,
                        data.subscriberPacketRouter,
                        data.settings)
      )
  }

  def serverConnect(data: ConnectReceived): Behavior[Event] = Behaviors.withTimers { timer =>
    timer.startSingleTimer("receive-connack", ReceiveConnAckTimeout, data.settings.receiveConnAckTimeout)

    def disconnect(): Behavior[Event] = {
      data.remote.complete()
      disconnected(
        Uninitialized(data.consumerPacketRouter, data.producerPacketRouter, data.subscriberPacketRouter, data.settings)
      )
    }

    Behaviors
      .receivePartial[Event] {
        case (context, ConnAckReceivedFromRemote(connAck, local))
            if connAck.returnCode.contains(ConnAckReturnCode.ConnectionAccepted) =>
          local ! ForwardConnAck(data.connectData)
          if (data.connect.connectFlags.contains(ConnectFlags.CleanSession))
            context.children.foreach(context.stop)
          data.stash.foreach(context.self.tell)
          serverConnected(
            ConnAckReceived(
              data.connect.keepAlive,
              pendingPingResp = false,
              Vector.empty,
              data.remote,
              data.consumerPacketRouter,
              data.producerPacketRouter,
              data.subscriberPacketRouter,
              data.settings
            )
          )
        case (_, ConnAckReceivedFromRemote(_, local)) =>
          local ! ForwardConnAck(data.connectData)
          disconnect()
        case (_, ReceiveConnAckTimeout) =>
          disconnect()
        case (_, e) if data.stash.size < data.settings.maxConnectStashSize =>
          serverConnect(data.copy(stash = data.stash :+ e))
      }
      .receiveSignal {
        case (_, PostStop) =>
          data.remote.complete()
          Behaviors.same
      }

  }

  def serverConnected(data: ConnAckReceived): Behavior[Event] = Behaviors.withTimers { timer =>
    if (data.keepAlive.toMillis > 0)
      timer.startSingleTimer("send-pingreq", SendPingReqTimeout, data.keepAlive)

    def disconnect(): Behavior[Event] = {
      data.remote.complete()
      disconnected(
        Uninitialized(data.consumerPacketRouter, data.producerPacketRouter, data.subscriberPacketRouter, data.settings)
      )
    }

    Behaviors
      .receivePartial[Event] {
        case (_, ConnectionLost) =>
          disconnect()
        case (_, DisconnectReceivedLocally(remote)) =>
          remote ! ForwardDisconnect
          disconnect()
        case (context, SubscribeReceivedLocally(subscribe, subscribeData, remote)) =>
          subscribe.topicFilters.foreach { topicFilter =>
            val (topicFilterName, _) = topicFilter
            val subscriberName = mkActorName(SubscriberNamePrefix + topicFilterName)
            context.child(subscriberName) match {
              case None =>
                context.spawn(
                  Subscriber(subscribeData, remote, data.subscriberPacketRouter, data.settings),
                  subscriberName
                )
              case _: Some[_] => // Ignored for existing subscriptions
            }
          }
          serverConnected(data)
        case (_, PublishReceivedFromRemote(publish, remote))
            if (publish.flags & ControlPacketFlags.QoSReserved).underlying == 0 =>
          remote ! Consumer.ForwardPublish
          serverConnected(data)
        case (context, PublishReceivedFromRemote(Publish(flags, topicName, Some(packetId), _), local)) =>
          val consumerName = mkActorName(ConsumerNamePrefix + topicName + packetId.underlying)
          context.child(consumerName) match {
            case None =>
              context.spawn(Consumer(packetId, flags, local, data.consumerPacketRouter, data.settings), consumerName)
            case _: Some[_] => // Ignored for existing consumptions
          }
          serverConnected(data)
        case (_, PublishReceivedLocally(publish, _, remote))
            if (publish.flags & ControlPacketFlags.QoSReserved).underlying == 0 =>
          remote ! Source.single(Producer.ForwardPublish(None, dup = false))
          serverConnected(data)
        case (context, prl @ PublishReceivedLocally(publish, publishData, remote)) =>
          val producerName = mkActorName(ProducerNamePrefix + publish.topicName)
          context.child(producerName) match {
            case None if !data.pendingLocalPublications.exists(_._1 == publish.topicName) =>
              context.watchWith(
                context.spawn(Producer(publish.flags, publishData, remote, data.producerPacketRouter, data.settings),
                              producerName),
                ProducerFree(publish.topicName)
              )
              serverConnected(data)
            case _ =>
              serverConnected(
                data.copy(pendingLocalPublications = data.pendingLocalPublications :+ (publish.topicName -> prl))
              )
          }
        case (context, ProducerFree(topicName)) =>
          val i = data.pendingLocalPublications.indexWhere(_._1 == topicName)
          if (i >= 0) {
            val prl = data.pendingLocalPublications(i)._2
            val producerName = mkActorName(ProducerNamePrefix + topicName)
            context.watchWith(
              context.spawn(
                Producer(prl.publish.flags, prl.publishData, prl.remote, data.producerPacketRouter, data.settings),
                producerName
              ),
              ProducerFree(topicName)
            )
            serverConnected(
              data.copy(
                pendingLocalPublications =
                data.pendingLocalPublications.take(i) ++ data.pendingLocalPublications.drop(i + 1)
              )
            )
          } else {
            serverConnected(data)
          }
        case (_, SendPingReqTimeout) if data.pendingPingResp =>
          data.remote.fail(PingFailed)
          disconnect()
        case (_, SendPingReqTimeout) =>
          data.remote.offer(ForwardPingReq)
          serverConnected(data.copy(pendingPingResp = true))
        case (_, PingRespReceivedFromRemote(local)) =>
          local ! ForwardPingResp
          serverConnected(data.copy(pendingPingResp = false))
      }
      .receiveSignal {
        case (_, PostStop) =>
          data.remote.complete()
          Behaviors.same
      }
  }
}

/*
 * A subscriber manages the client state in relation to having made a
 * subscription to a a server-side topic. A subscriber is created
 * per server per topic.
 */
@InternalApi private[streaming] object Subscriber {

  type SubscribeData = Option[_]

  /*
   * Construct with the starting state
   */
  def apply(subscribeData: SubscribeData,
            remote: ActorRef[ForwardSubscribe],
            packetRouter: ActorRef[LocalPacketRouter.Request[Event]],
            settings: MqttSessionSettings): Behavior[Event] =
    prepareServerSubscribe(Start(subscribeData, remote, packetRouter, settings))

  // Our FSM data, FSM events and commands emitted by the FSM

  sealed abstract class Data(val settings: MqttSessionSettings)
  final case class Start(subscribeData: SubscribeData,
                         remote: ActorRef[ForwardSubscribe],
                         packetRouter: ActorRef[LocalPacketRouter.Request[Event]],
                         override val settings: MqttSessionSettings)
      extends Data(settings)
  final case class ServerSubscribe(packetId: PacketId,
                                   subscribeData: SubscribeData,
                                   packetRouter: ActorRef[LocalPacketRouter.Request[Event]],
                                   override val settings: MqttSessionSettings)
      extends Data(settings)

  sealed abstract class Event
  final case class AcquiredPacketId(packetId: PacketId) extends Event
  final case object UnobtainablePacketId extends Event
  final case class SubAckReceivedFromRemote(local: ActorRef[ForwardSubAck]) extends Event
  case object ReceiveSubAckTimeout extends Event

  sealed abstract class Command
  final case class ForwardSubscribe(packetId: PacketId) extends Command
  final case class ForwardSubAck(connectData: SubscribeData) extends Command

  // State event handling

  def prepareServerSubscribe(data: Start): Behavior[Event] = Behaviors.setup { context =>
    implicit val actorMqttSessionTimeout: Timeout = data.settings.actorMqttSessionTimeout

    context.ask[LocalPacketRouter.Register[Event], LocalPacketRouter.Registered](data.packetRouter)(
      replyTo => LocalPacketRouter.Register(context.self, replyTo)
    ) {
      case Success(registered: LocalPacketRouter.Registered) => AcquiredPacketId(registered.packetId)
      case Failure(_) => UnobtainablePacketId
    }

    Behaviors.receiveMessagePartial {
      case AcquiredPacketId(packetId) =>
        data.remote ! ForwardSubscribe(packetId)
        serverSubscribe(
          ServerSubscribe(packetId, data.subscribeData, data.packetRouter, data.settings)
        )
      case UnobtainablePacketId =>
        Behaviors.stopped
    }
  }

  def serverSubscribe(data: ServerSubscribe): Behavior[Event] = Behaviors.withTimers { timer =>
    timer.startSingleTimer("receive-suback", ReceiveSubAckTimeout, data.settings.receiveSubAckTimeout)

    Behaviors
      .receiveMessagePartial[Event] {
        case SubAckReceivedFromRemote(local) =>
          local ! ForwardSubAck(data.subscribeData)
          Behaviors.stopped
        case ReceiveSubAckTimeout =>
          Behaviors.stopped
      }
      .receiveSignal {
        case (_, PostStop) =>
          data.packetRouter ! LocalPacketRouter.Unregister(data.packetId)
          Behaviors.same
      }
  }
}

/*
 * A producer manages the client state in relation to publishing to a server-side topic.
 *
 * Producers are slightly special in that they should do all that they can to ensure that
 * a PUBLISH message gets through. Hence, retries are indefinite.
 *
 * A producer is created per server per topic.
 */
@InternalApi private[streaming] object Producer {

  type PublishData = Option[_]

  /*
   * Construct with the starting state
   */
  def apply(flags: ControlPacketFlags,
            publishData: PublishData,
            remote: ActorRef[Source[ForwardPublishingCommand, NotUsed]],
            packetRouter: ActorRef[LocalPacketRouter.Request[Event]],
            settings: MqttSessionSettings): Behavior[Event] =
    preparePublish(Start(flags, publishData, remote, packetRouter, settings))

  // Our FSM data, FSM events and commands emitted by the FSM

  sealed abstract class Data(val settings: MqttSessionSettings)
  final case class Start(flags: ControlPacketFlags,
                         publishData: PublishData,
                         remote: ActorRef[Source[ForwardPublishingCommand, NotUsed]],
                         packetRouter: ActorRef[LocalPacketRouter.Request[Event]],
                         override val settings: MqttSessionSettings)
      extends Data(settings)
  final case class Publishing(remote: SourceQueueWithComplete[ForwardPublishingCommand],
                              flags: ControlPacketFlags,
                              packetId: PacketId,
                              publishData: PublishData,
                              packetRouter: ActorRef[LocalPacketRouter.Request[Event]],
                              override val settings: MqttSessionSettings)
      extends Data(settings)

  sealed abstract class Event
  final case class AcquiredPacketId(packetId: PacketId) extends Event
  final case object UnacquiredPacketId extends Event
  case object ReceivePubAckRecTimeout extends Event
  final case class PubAckReceivedFromRemote(local: ActorRef[ForwardPubAck]) extends Event
  final case class PubRecReceivedFromRemote(local: ActorRef[ForwardPubRec]) extends Event
  case object ReceivePubCompTimeout extends Event
  final case class PubCompReceivedFromRemote(local: ActorRef[ForwardPubComp]) extends Event

  sealed abstract class Command
  sealed abstract class ForwardPublishingCommand extends Command
  final case class ForwardPublish(packetId: Option[PacketId], dup: Boolean) extends ForwardPublishingCommand
  final case class ForwardPubAck(publishData: PublishData) extends Command
  final case class ForwardPubRec(publishData: PublishData) extends Command
  final case class ForwardPubRel(packetId: PacketId) extends ForwardPublishingCommand
  final case class ForwardPubComp(publishData: PublishData) extends Command

  // State event handling

  def preparePublish(data: Start): Behavior[Event] = Behaviors.setup { context =>
    implicit val actorMqttSessionTimeout: Timeout = data.settings.actorMqttSessionTimeout

    def requestPacketId(): Unit =
      context.ask[LocalPacketRouter.Register[Event], LocalPacketRouter.Registered](data.packetRouter)(
        replyTo => LocalPacketRouter.Register(context.self.upcast, replyTo)
      ) {
        case Success(acquired: LocalPacketRouter.Registered) => AcquiredPacketId(acquired.packetId)
        case Failure(_) => UnacquiredPacketId
      }

    requestPacketId()

    implicit val mat: Materializer = ActorMaterializer()(context.system)
    val (queue, source) = Source
      .queue[ForwardPublishingCommand](1, OverflowStrategy.dropHead)
      .toMat(BroadcastHub.sink)(Keep.both)
      .run()
    data.remote ! source

    Behaviors
      .receiveMessagePartial[Event] {
        case AcquiredPacketId(packetId) =>
          queue.offer(ForwardPublish(Some(packetId), dup = false))
          publishUnacknowledged(
            Publishing(queue, data.flags, packetId, data.publishData, data.packetRouter, data.settings)
          )
        case UnacquiredPacketId =>
          requestPacketId()
          Behaviors.same
      }
      .receiveSignal {
        case (_, PostStop) =>
          queue.complete()
          Behaviors.same
      }
  }

  def publishUnacknowledged(data: Publishing): Behavior[Event] = Behaviors.withTimers { timer =>
    timer.startSingleTimer("receive-pubackrec", ReceivePubAckRecTimeout, data.settings.receivePubAckRecTimeout)

    Behaviors
      .receiveMessagePartial[Event] {
        case PubAckReceivedFromRemote(local) if data.flags.contains(ControlPacketFlags.QoSAtLeastOnceDelivery) =>
          local ! ForwardPubAck(data.publishData)
          Behaviors.stopped
        case PubRecReceivedFromRemote(local) if data.flags.contains(ControlPacketFlags.QoSAtMostOnceDelivery) =>
          local ! ForwardPubRec(data.publishData)
          publishAcknowledged(data)
        case ReceivePubAckRecTimeout =>
          data.remote.offer(ForwardPublish(Some(data.packetId), dup = true))
          publishUnacknowledged(data)
      }
      .receiveSignal {
        case (_, PostStop) =>
          data.packetRouter ! LocalPacketRouter.Unregister(data.packetId)
          data.remote.complete()
          Behaviors.same
      }
  }

  def publishAcknowledged(data: Publishing): Behavior[Event] = Behaviors.withTimers { timer =>
    timer.startSingleTimer("receive-pubrel", ReceivePubCompTimeout, data.settings.receivePubCompTimeout)

    data.remote.offer(ForwardPubRel(data.packetId))

    Behaviors
      .receiveMessagePartial[Event] {
        case PubCompReceivedFromRemote(local) =>
          local ! ForwardPubComp(data.publishData)
          Behaviors.stopped
        case ReceivePubCompTimeout =>
          data.remote.offer(ForwardPubRel(data.packetId))
          publishAcknowledged(data)
      }
      .receiveSignal {
        case (_, PostStop) =>
          data.packetRouter ! LocalPacketRouter.Unregister(data.packetId)
          data.remote.complete()
          Behaviors.same
      }
  }
}

/*
 * A consumer manages the client state in relation to having made a
 * subscription to a server-side topic. A consumer is created
 * per server per topic per packet id.
 */
@InternalApi private[streaming] object Consumer {

  /*
   * Construct with the starting state
   */
  def apply(packetId: PacketId,
            flags: ControlPacketFlags,
            local: ActorRef[ForwardPublish.type],
            packetRouter: ActorRef[RemotePacketRouter.Request[Event]],
            settings: MqttSessionSettings): Behavior[Event] =
    prepareClientConsumption(Start(packetId, flags, local, packetRouter, settings))

  // Our FSM data, FSM events and commands emitted by the FSM

  sealed abstract class Data(val packetId: PacketId,
                             val flags: ControlPacketFlags,
                             val packetRouter: ActorRef[RemotePacketRouter.Request[Event]],
                             val settings: MqttSessionSettings)
  final case class Start(override val packetId: PacketId,
                         override val flags: ControlPacketFlags,
                         local: ActorRef[ForwardPublish.type],
                         override val packetRouter: ActorRef[RemotePacketRouter.Request[Event]],
                         override val settings: MqttSessionSettings)
      extends Data(packetId, flags, packetRouter, settings)
  final case class ClientConsuming(override val packetId: PacketId,
                                   override val flags: ControlPacketFlags,
                                   override val packetRouter: ActorRef[RemotePacketRouter.Request[Event]],
                                   override val settings: MqttSessionSettings)
      extends Data(packetId, flags, packetRouter, settings)

  sealed abstract class Event
  final case object RegisteredPacketId extends Event
  final case object UnobtainablePacketId extends Event
  final case class PubAckReceivedLocally(remote: ActorRef[ForwardPubAck.type]) extends Event
  final case class PubRecReceivedLocally(remote: ActorRef[ForwardPubRec.type]) extends Event
  case object ReceivePubAckRecTimeout extends Event
  final case class PubRelReceivedFromRemote(local: ActorRef[ForwardPubRel.type]) extends Event
  case object ReceivePubRelTimeout extends Event
  final case class PubCompReceivedLocally(remote: ActorRef[ForwardPubComp.type]) extends Event
  case object ReceivePubCompTimeout extends Event

  sealed abstract class Command
  case object ForwardPublish extends Command
  case object ForwardPubAck extends Command
  case object ForwardPubRec extends Command
  case object ForwardPubRel extends Command
  case object ForwardPubComp extends Command

  // State event handling

  def prepareClientConsumption(data: Start): Behavior[Event] = Behaviors.setup { context =>
    implicit val actorMqttSessionTimeout: Timeout = data.settings.actorMqttSessionTimeout

    context.ask[RemotePacketRouter.Register[Event], RemotePacketRouter.Registered.type](data.packetRouter)(
      replyTo => RemotePacketRouter.Register(context.self, data.packetId, replyTo)
    ) {
      case Success(RemotePacketRouter.Registered) => RegisteredPacketId
      case Failure(_) => UnobtainablePacketId
    }

    Behaviors.receiveMessagePartial {
      case RegisteredPacketId =>
        data.local ! ForwardPublish
        consumeUnacknowledged(ClientConsuming(data.packetId, data.flags, data.packetRouter, data.settings))
      case UnobtainablePacketId =>
        Behaviors.stopped
    }
  }

  def consumeUnacknowledged(data: ClientConsuming): Behavior[Event] = Behaviors.withTimers { timer =>
    timer.startSingleTimer("receive-pubackrel", ReceivePubAckRecTimeout, data.settings.receivePubAckRecTimeout)
    Behaviors
      .receiveMessagePartial[Event] {
        case PubAckReceivedLocally(remote) if data.flags.contains(ControlPacketFlags.QoSAtLeastOnceDelivery) =>
          remote ! ForwardPubAck
          Behaviors.stopped
        case PubRecReceivedLocally(remote) if data.flags.contains(ControlPacketFlags.QoSExactlyOnceDelivery) =>
          remote ! ForwardPubRec
          consumeReceived(data)
        case ReceivePubAckRecTimeout =>
          Behaviors.stopped
      }
      .receiveSignal {
        case (_, PostStop) =>
          data.packetRouter ! RemotePacketRouter.Unregister(data.packetId)
          Behaviors.same
      }
  }

  def consumeReceived(data: ClientConsuming): Behavior[Event] = Behaviors.withTimers { timer =>
    timer.startSingleTimer("receive-pubrel", ReceivePubRelTimeout, data.settings.receivePubRelTimeout)
    Behaviors
      .receiveMessagePartial[Event] {
        case PubRelReceivedFromRemote(local) =>
          local ! ForwardPubRel
          consumeAcknowledged(data)
        case ReceivePubRelTimeout =>
          Behaviors.stopped
      }
      .receiveSignal {
        case (_, PostStop) =>
          data.packetRouter ! RemotePacketRouter.Unregister(data.packetId)
          Behaviors.same
      }
  }

  def consumeAcknowledged(data: ClientConsuming): Behavior[Event] = Behaviors.withTimers { timer =>
    timer.startSingleTimer("receive-pubcomp", ReceivePubCompTimeout, data.settings.receivePubCompTimeout)
    Behaviors
      .receiveMessagePartial[Event] {
        case PubCompReceivedLocally(remote) =>
          remote ! ForwardPubComp
          Behaviors.stopped
        case ReceivePubRelTimeout =>
          Behaviors.stopped
      }
      .receiveSignal {
        case (_, PostStop) =>
          data.packetRouter ! RemotePacketRouter.Unregister(data.packetId)
          Behaviors.same
      }
  }
}

@InternalApi private[streaming] object LocalPacketRouter {
  /*
   * In case some brokers treat 0 as no packet id, we set our min to 1
   * e.g. https://renesasrulz.com/synergy/synergy_tech_notes/f/technical-bulletin-board-notification-postings/8998/mqtt-client-packet-identifier-is-0-by-default-which-causes-azure-iot-hub-to-reset-connection
   */
  private val MinPacketId = PacketId(1)
  private val MaxPacketId = PacketId(0xffff)

  // Requests

  sealed abstract class Request[A]
  final case class Register[A](registrant: ActorRef[A], replyTo: ActorRef[Registered]) extends Request[A]
  final case class Unregister[A](packetId: PacketId) extends Request[A]
  final case class Route[A](packetId: PacketId, event: A) extends Request[A]

  // Replies

  sealed abstract class Reply
  final case class Registered(packetId: PacketId) extends Reply

  /*
   * Construct with the starting state
   */
  def apply[A]: Behavior[Request[A]] =
    new LocalPacketRouter[A].main(Map.empty, MinPacketId)
}

/*
 * Route locally generated MQTT packets based on packet identifiers.
 * Callers are able to request that they be registered for routing and,
 * in return, receive the packet identifier acquired. These
 * callers then release packet identifiers so that they may then
 * be re-used.
 *
 * The acquisition algorithm is optimised to return newly allocated
 * packet ids fast, and take the cost when releasing them as
 * the caller isn't waiting on a reply.
 */
@InternalApi private[streaming] class LocalPacketRouter[A] {

  import LocalPacketRouter._

  // Processing

  def main(registrantsByPacketId: Map[PacketId, ActorRef[A]], nextPacketId: PacketId): Behavior[Request[A]] =
    Behaviors.receiveMessage {
      case Register(registrant: ActorRef[A], replyTo) if nextPacketId.underlying <= MaxPacketId.underlying =>
        replyTo ! Registered(nextPacketId)
        main(registrantsByPacketId + (nextPacketId -> registrant), PacketId(nextPacketId.underlying + 1))
      case _: Register[A] =>
        Behaviors.same // We cannot allocate any more. This will eventually cause a timeout to occur on the requestor.
      case Unregister(packetId) =>
        val remainingPacketIds = registrantsByPacketId - packetId
        val revisedNextPacketId = if (remainingPacketIds.nonEmpty) {
          val maxPacketId = PacketId(remainingPacketIds.keys.map(_.underlying).max)
          PacketId(maxPacketId.underlying + 1)
        } else {
          MinPacketId
        }
        main(remainingPacketIds, revisedNextPacketId)
      case Route(packetId, event) =>
        registrantsByPacketId.get(packetId).foreach(_.tell(event))
        Behaviors.same
    }
}

@InternalApi private[streaming] object RemotePacketRouter {
  // Requests

  sealed abstract class Request[A]
  final case class Register[A](registrant: ActorRef[A], packetId: PacketId, replyTo: ActorRef[Registered.type])
      extends Request[A]
  final case class Unregister[A](packetId: PacketId) extends Request[A]
  final case class Route[A](packetId: PacketId, event: A) extends Request[A]

  // Replies

  sealed abstract class Reply
  final case object Registered extends Reply

  /*
   * Construct with the starting state
   */
  def apply[A]: Behavior[Request[A]] =
    new RemotePacketRouter[A].main(Map.empty)
}

/*
 * Route remotely generated MQTT packets based on packet identifiers.
 * Callers are able to request that they be registered for routing
 * along with a packet id received from the remote.
 */
@InternalApi private[streaming] class RemotePacketRouter[A] {

  import RemotePacketRouter._

  // Processing

  def main(registrantsByPacketId: Map[PacketId, ActorRef[A]]): Behavior[Request[A]] =
    Behaviors.receiveMessage {
      case Register(registrant: ActorRef[A], packetId, replyTo) =>
        replyTo ! Registered
        main(registrantsByPacketId + (packetId -> registrant))
      case Unregister(packetId) =>
        main(registrantsByPacketId - packetId)
      case Route(packetId, event) =>
        registrantsByPacketId.get(packetId).foreach(_.tell(event))
        Behaviors.same
    }
}
