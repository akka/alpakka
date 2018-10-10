/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.mqtt.streaming
package impl

import java.nio.charset.StandardCharsets

import akka.NotUsed
import akka.actor.typed.{ActorRef, Behavior, PostStop}
import akka.actor.typed.scaladsl.{ActorContext, Behaviors}
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
            unsubscriberPacketRouter: ActorRef[LocalPacketRouter.Request[Unsubscriber.Event]],
            settings: MqttSessionSettings): Behavior[Event] =
    disconnected(
      Uninitialized(consumerPacketRouter,
                    producerPacketRouter,
                    subscriberPacketRouter,
                    unsubscriberPacketRouter,
                    settings)
    )

  // Our FSM data, FSM events and commands emitted by the FSM

  sealed abstract class Data(val consumerPacketRouter: ActorRef[RemotePacketRouter.Request[Consumer.Event]],
                             val producerPacketRouter: ActorRef[LocalPacketRouter.Request[Producer.Event]],
                             val subscriberPacketRouter: ActorRef[LocalPacketRouter.Request[Subscriber.Event]],
                             val unsubscriberPacketRouter: ActorRef[LocalPacketRouter.Request[Unsubscriber.Event]],
                             val settings: MqttSessionSettings)
  final case class Uninitialized(
      override val consumerPacketRouter: ActorRef[RemotePacketRouter.Request[Consumer.Event]],
      override val producerPacketRouter: ActorRef[LocalPacketRouter.Request[Producer.Event]],
      override val subscriberPacketRouter: ActorRef[LocalPacketRouter.Request[Subscriber.Event]],
      override val unsubscriberPacketRouter: ActorRef[LocalPacketRouter.Request[Unsubscriber.Event]],
      override val settings: MqttSessionSettings
  ) extends Data(consumerPacketRouter,
                   producerPacketRouter,
                   subscriberPacketRouter,
                   unsubscriberPacketRouter,
                   settings)
  final case class ConnectReceived(
      connect: Connect,
      connectData: ConnectData,
      stash: Seq[Event],
      remote: SourceQueueWithComplete[ForwardConnectCommand],
      override val consumerPacketRouter: ActorRef[RemotePacketRouter.Request[Consumer.Event]],
      override val producerPacketRouter: ActorRef[LocalPacketRouter.Request[Producer.Event]],
      override val subscriberPacketRouter: ActorRef[LocalPacketRouter.Request[Subscriber.Event]],
      override val unsubscriberPacketRouter: ActorRef[LocalPacketRouter.Request[Unsubscriber.Event]],
      override val settings: MqttSessionSettings
  ) extends Data(consumerPacketRouter,
                   producerPacketRouter,
                   subscriberPacketRouter,
                   unsubscriberPacketRouter,
                   settings)
  final case class ConnAckReceived(
      connectFlags: ConnectFlags,
      keepAlive: FiniteDuration,
      pendingPingResp: Boolean,
      pendingLocalPublications: Seq[(String, PublishReceivedLocally)],
      remote: SourceQueueWithComplete[ForwardConnectCommand],
      override val consumerPacketRouter: ActorRef[RemotePacketRouter.Request[Consumer.Event]],
      override val producerPacketRouter: ActorRef[LocalPacketRouter.Request[Producer.Event]],
      override val subscriberPacketRouter: ActorRef[LocalPacketRouter.Request[Subscriber.Event]],
      override val unsubscriberPacketRouter: ActorRef[LocalPacketRouter.Request[Unsubscriber.Event]],
      override val settings: MqttSessionSettings
  ) extends Data(consumerPacketRouter,
                   producerPacketRouter,
                   subscriberPacketRouter,
                   unsubscriberPacketRouter,
                   settings)

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
  final case class UnsubscribeReceivedLocally(unsubscribe: Unsubscribe,
                                              unsubscribeData: Unsubscriber.UnsubscribeData,
                                              remote: ActorRef[Unsubscriber.ForwardUnsubscribe])
      extends Event

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
  private val UnsubscriberNamePrefix = "unsubscriber-"

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
        ConnectReceived(
          connect,
          connectData,
          Vector.empty,
          queue,
          data.consumerPacketRouter,
          data.producerPacketRouter,
          data.subscriberPacketRouter,
          data.unsubscriberPacketRouter,
          data.settings
        )
      )
  }

  def disconnect(context: ActorContext[Event],
                 connectFlags: ConnectFlags,
                 remote: SourceQueueWithComplete[ForwardConnectCommand],
                 data: Data): Behavior[Event] = {
    if (connectFlags.contains(ConnectFlags.CleanSession))
      context.children.foreach(context.stop)

    remote.complete()

    disconnected(
      Uninitialized(data.consumerPacketRouter,
                    data.producerPacketRouter,
                    data.subscriberPacketRouter,
                    data.unsubscriberPacketRouter,
                    data.settings)
    )
  }

  def serverConnect(data: ConnectReceived): Behavior[Event] = Behaviors.withTimers { timer =>
    timer.startSingleTimer("receive-connack", ReceiveConnAckTimeout, data.settings.receiveConnAckTimeout)

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
              data.connect.connectFlags,
              data.connect.keepAlive,
              pendingPingResp = false,
              Vector.empty,
              data.remote,
              data.consumerPacketRouter,
              data.producerPacketRouter,
              data.subscriberPacketRouter,
              data.unsubscriberPacketRouter,
              data.settings
            )
          )
        case (context, ConnAckReceivedFromRemote(_, local)) =>
          local ! ForwardConnAck(data.connectData)
          disconnect(context, data.connect.connectFlags, data.remote, data)
        case (context, ReceiveConnAckTimeout) =>
          disconnect(context, data.connect.connectFlags, data.remote, data)
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
    else
      data.remote.complete() // We'll never be sending pings so free up the command channel for other things

    Behaviors
      .receivePartial[Event] {
        case (context, ConnectionLost) =>
          disconnect(context, data.connectFlags, data.remote, data)
        case (context, DisconnectReceivedLocally(remote)) =>
          remote ! ForwardDisconnect
          disconnect(context, data.connectFlags, data.remote, data)
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
        case (context, UnsubscribeReceivedLocally(unsubscribe, unsubscribeData, remote)) =>
          unsubscribe.topicFilters.foreach { topicFilter =>
            val unsubscriberName = mkActorName(UnsubscriberNamePrefix + topicFilter)
            context.child(unsubscriberName) match {
              case None =>
                context.spawn(
                  Unsubscriber(unsubscribeData, remote, data.unsubscriberPacketRouter, data.settings),
                  unsubscriberName
                )
              case _: Some[_] => // Ignored for existing unsubscriptions
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
        case (context, SendPingReqTimeout) if data.pendingPingResp =>
          data.remote.fail(PingFailed)
          disconnect(context, data.connectFlags, data.remote, data)
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
 * subscription to a server-side topic. A subscriber is created
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
 * A unsubscriber manages the client state in relation to unsubscribing from a
 * server-side topic. A unsubscriber is created per server per topic.
 */
@InternalApi private[streaming] object Unsubscriber {

  type UnsubscribeData = Option[_]

  /*
   * Construct with the starting state
   */
  def apply(unsubscribeData: UnsubscribeData,
            remote: ActorRef[ForwardUnsubscribe],
            packetRouter: ActorRef[LocalPacketRouter.Request[Event]],
            settings: MqttSessionSettings): Behavior[Event] =
    prepareServerUnsubscribe(Start(unsubscribeData, remote, packetRouter, settings))

  // Our FSM data, FSM events and commands emitted by the FSM

  sealed abstract class Data(val settings: MqttSessionSettings)
  final case class Start(unsubscribeData: UnsubscribeData,
                         remote: ActorRef[ForwardUnsubscribe],
                         packetRouter: ActorRef[LocalPacketRouter.Request[Event]],
                         override val settings: MqttSessionSettings)
      extends Data(settings)
  final case class ServerUnsubscribe(packetId: PacketId,
                                     unsubscribeData: UnsubscribeData,
                                     packetRouter: ActorRef[LocalPacketRouter.Request[Event]],
                                     override val settings: MqttSessionSettings)
      extends Data(settings)

  sealed abstract class Event
  final case class AcquiredPacketId(packetId: PacketId) extends Event
  final case object UnobtainablePacketId extends Event
  final case class UnsubAckReceivedFromRemote(local: ActorRef[ForwardUnsubAck]) extends Event
  case object ReceiveUnsubAckTimeout extends Event

  sealed abstract class Command
  final case class ForwardUnsubscribe(packetId: PacketId) extends Command
  final case class ForwardUnsubAck(connectData: UnsubscribeData) extends Command

  // State event handling

  def prepareServerUnsubscribe(data: Start): Behavior[Event] = Behaviors.setup { context =>
    implicit val actorMqttSessionTimeout: Timeout = data.settings.actorMqttSessionTimeout

    context.ask[LocalPacketRouter.Register[Event], LocalPacketRouter.Registered](data.packetRouter)(
      replyTo => LocalPacketRouter.Register(context.self, replyTo)
    ) {
      case Success(registered: LocalPacketRouter.Registered) => AcquiredPacketId(registered.packetId)
      case Failure(_) => UnobtainablePacketId
    }

    Behaviors.receiveMessagePartial {
      case AcquiredPacketId(packetId) =>
        data.remote ! ForwardUnsubscribe(packetId)
        serverUnsubscribe(
          ServerUnsubscribe(packetId, data.unsubscribeData, data.packetRouter, data.settings)
        )
      case UnobtainablePacketId =>
        Behaviors.stopped
    }
  }

  def serverUnsubscribe(data: ServerUnsubscribe): Behavior[Event] = Behaviors.withTimers { timer =>
    timer.startSingleTimer("receive-unsubAck", ReceiveUnsubAckTimeout, data.settings.receiveUnsubAckTimeout)

    Behaviors
      .receiveMessagePartial[Event] {
        case UnsubAckReceivedFromRemote(local) =>
          local ! ForwardUnsubAck(data.unsubscribeData)
          Behaviors.stopped
        case ReceiveUnsubAckTimeout =>
          Behaviors.stopped
      }
      .receiveSignal {
        case (_, PostStop) =>
          data.packetRouter ! LocalPacketRouter.Unregister(data.packetId)
          Behaviors.same
      }
  }
}
