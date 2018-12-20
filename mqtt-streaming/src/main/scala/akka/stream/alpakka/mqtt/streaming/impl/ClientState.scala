/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.mqtt.streaming
package impl

import akka.NotUsed
import akka.actor.typed.{ActorRef, Behavior, PostStop, Terminated}
import akka.actor.typed.scaladsl.{ActorContext, Behaviors}
import akka.annotation.InternalApi
import akka.stream.{Materializer, OverflowStrategy}
import akka.stream.scaladsl.{BroadcastHub, Keep, Sink, Source, SourceQueueWithComplete}

import scala.concurrent.Promise
import scala.concurrent.duration.FiniteDuration
import scala.util.control.NoStackTrace
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
  case object PingFailed extends Exception with NoStackTrace

  /*
   * Construct with the starting state
   */
  def apply(consumerPacketRouter: ActorRef[RemotePacketRouter.Request[Consumer.Event]],
            producerPacketRouter: ActorRef[LocalPacketRouter.Request[Producer.Event]],
            subscriberPacketRouter: ActorRef[LocalPacketRouter.Request[Subscriber.Event]],
            unsubscriberPacketRouter: ActorRef[LocalPacketRouter.Request[Unsubscriber.Event]],
            settings: MqttSessionSettings)(implicit mat: Materializer): Behavior[Event] =
    disconnected(
      Uninitialized(Vector.empty,
                    consumerPacketRouter,
                    producerPacketRouter,
                    subscriberPacketRouter,
                    unsubscriberPacketRouter,
                    settings)
    )

  // Our FSM data, FSM events and commands emitted by the FSM

  sealed abstract class Data(val stash: Seq[Event],
                             val consumerPacketRouter: ActorRef[RemotePacketRouter.Request[Consumer.Event]],
                             val producerPacketRouter: ActorRef[LocalPacketRouter.Request[Producer.Event]],
                             val subscriberPacketRouter: ActorRef[LocalPacketRouter.Request[Subscriber.Event]],
                             val unsubscriberPacketRouter: ActorRef[LocalPacketRouter.Request[Unsubscriber.Event]],
                             val settings: MqttSessionSettings)
  final case class Uninitialized(
      override val stash: Seq[Event],
      override val consumerPacketRouter: ActorRef[RemotePacketRouter.Request[Consumer.Event]],
      override val producerPacketRouter: ActorRef[LocalPacketRouter.Request[Producer.Event]],
      override val subscriberPacketRouter: ActorRef[LocalPacketRouter.Request[Subscriber.Event]],
      override val unsubscriberPacketRouter: ActorRef[LocalPacketRouter.Request[Unsubscriber.Event]],
      override val settings: MqttSessionSettings
  ) extends Data(stash,
                   consumerPacketRouter,
                   producerPacketRouter,
                   subscriberPacketRouter,
                   unsubscriberPacketRouter,
                   settings)
  final case class ConnectReceived(
      connect: Connect,
      connectData: ConnectData,
      remote: SourceQueueWithComplete[ForwardConnectCommand],
      override val stash: Seq[Event],
      override val consumerPacketRouter: ActorRef[RemotePacketRouter.Request[Consumer.Event]],
      override val producerPacketRouter: ActorRef[LocalPacketRouter.Request[Producer.Event]],
      override val subscriberPacketRouter: ActorRef[LocalPacketRouter.Request[Subscriber.Event]],
      override val unsubscriberPacketRouter: ActorRef[LocalPacketRouter.Request[Unsubscriber.Event]],
      override val settings: MqttSessionSettings
  ) extends Data(stash,
                   consumerPacketRouter,
                   producerPacketRouter,
                   subscriberPacketRouter,
                   unsubscriberPacketRouter,
                   settings)
  final case class ConnAckReceived(
      connectFlags: ConnectFlags,
      keepAlive: FiniteDuration,
      pendingPingResp: Boolean,
      activeConsumers: Set[String],
      activeProducers: Set[String],
      pendingLocalPublications: Seq[(String, PublishReceivedLocally)],
      pendingRemotePublications: Seq[(String, PublishReceivedFromRemote)],
      remote: SourceQueueWithComplete[ForwardConnectCommand],
      override val stash: Seq[Event],
      override val consumerPacketRouter: ActorRef[RemotePacketRouter.Request[Consumer.Event]],
      override val producerPacketRouter: ActorRef[LocalPacketRouter.Request[Producer.Event]],
      override val subscriberPacketRouter: ActorRef[LocalPacketRouter.Request[Subscriber.Event]],
      override val unsubscriberPacketRouter: ActorRef[LocalPacketRouter.Request[Unsubscriber.Event]],
      override val settings: MqttSessionSettings
  ) extends Data(stash,
                   consumerPacketRouter,
                   producerPacketRouter,
                   subscriberPacketRouter,
                   unsubscriberPacketRouter,
                   settings)
  final case class PendingSubscribe(
      connectFlags: ConnectFlags,
      keepAlive: FiniteDuration,
      pendingPingResp: Boolean,
      activeConsumers: Set[String],
      activeProducers: Set[String],
      pendingLocalPublications: Seq[(String, PublishReceivedLocally)],
      pendingRemotePublications: Seq[(String, PublishReceivedFromRemote)],
      remote: SourceQueueWithComplete[ForwardConnectCommand],
      override val stash: Seq[Event],
      override val consumerPacketRouter: ActorRef[RemotePacketRouter.Request[Consumer.Event]],
      override val producerPacketRouter: ActorRef[LocalPacketRouter.Request[Producer.Event]],
      override val subscriberPacketRouter: ActorRef[LocalPacketRouter.Request[Subscriber.Event]],
      override val unsubscriberPacketRouter: ActorRef[LocalPacketRouter.Request[Unsubscriber.Event]],
      override val settings: MqttSessionSettings
  ) extends Data(stash,
                   consumerPacketRouter,
                   producerPacketRouter,
                   subscriberPacketRouter,
                   unsubscriberPacketRouter,
                   settings)

  sealed abstract class Event
  final case class ConnectReceivedLocally(connect: Connect,
                                          connectData: ConnectData,
                                          remote: Promise[Source[ForwardConnectCommand, NotUsed]])
      extends Event
  final case class ConnAckReceivedFromRemote(connAck: ConnAck, local: Promise[ForwardConnAck]) extends Event
  case object ReceiveConnAckTimeout extends Event
  case object ConnectionLost extends Event
  final case class DisconnectReceivedLocally(remote: Promise[ForwardDisconnect.type]) extends Event
  final case class SubscribeReceivedLocally(subscribe: Subscribe,
                                            subscribeData: Subscriber.SubscribeData,
                                            remote: Promise[Subscriber.ForwardSubscribe])
      extends Event
  final case class PublishReceivedFromRemote(publish: Publish, local: Promise[Consumer.ForwardPublish.type])
      extends Event
  final case class ConsumerFree(topicName: String) extends Event
  final case class PublishReceivedLocally(publish: Publish, publishData: Producer.PublishData) extends Event
  final case class ProducerFree(topicName: String) extends Event
  case object SendPingReqTimeout extends Event
  final case class PingRespReceivedFromRemote(local: Promise[ForwardPingResp.type]) extends Event
  final case class ReceivedProducerPublishingCommand(command: Source[Producer.ForwardPublishingCommand, NotUsed])
      extends Event
  final case class UnsubscribeReceivedLocally(unsubscribe: Unsubscribe,
                                              unsubscribeData: Unsubscriber.UnsubscribeData,
                                              remote: Promise[Unsubscriber.ForwardUnsubscribe])
      extends Event

  sealed abstract class Command
  sealed abstract class ForwardConnectCommand
  case object ForwardConnect extends ForwardConnectCommand
  case object ForwardPingReq extends ForwardConnectCommand
  final case class ForwardPublish(publish: Publish, packetId: Option[PacketId]) extends ForwardConnectCommand
  final case class ForwardPubRel(packetId: PacketId) extends ForwardConnectCommand
  final case class ForwardConnAck(connectData: ConnectData) extends Command
  case object ForwardDisconnect extends Command
  case object ForwardPingResp extends Command

  // State event handling

  private val ConsumerNamePrefix = "consumer-"
  private val ProducerNamePrefix = "producer-"
  private val SubscriberNamePrefix = "subscriber-"
  private val UnsubscriberNamePrefix = "unsubscriber-"

  def disconnected(data: Uninitialized)(implicit mat: Materializer): Behavior[Event] = Behaviors.receivePartial {
    case (context, ConnectReceivedLocally(connect, connectData, remote)) =>
      val (queue, source) = Source
        .queue[ForwardConnectCommand](1, OverflowStrategy.dropHead)
        .toMat(BroadcastHub.sink)(Keep.both)
        .run()
      remote.success(source)

      queue.offer(ForwardConnect)
      data.stash.foreach(context.self.tell)
      serverConnect(
        ConnectReceived(
          connect,
          connectData,
          queue,
          Vector.empty,
          data.consumerPacketRouter,
          data.producerPacketRouter,
          data.subscriberPacketRouter,
          data.unsubscriberPacketRouter,
          data.settings
        )
      )
    case (_, e) =>
      disconnected(data.copy(stash = data.stash :+ e))
  }

  def disconnect(context: ActorContext[Event],
                 connectFlags: ConnectFlags,
                 remote: SourceQueueWithComplete[ForwardConnectCommand],
                 data: Data)(implicit mat: Materializer): Behavior[Event] = {
    if (connectFlags.contains(ConnectFlags.CleanSession))
      context.children.foreach(context.stop)

    remote.complete()

    disconnected(
      Uninitialized(data.stash,
                    data.consumerPacketRouter,
                    data.producerPacketRouter,
                    data.subscriberPacketRouter,
                    data.unsubscriberPacketRouter,
                    data.settings)
    )
  }

  def serverConnect(data: ConnectReceived)(implicit mat: Materializer): Behavior[Event] = Behaviors.withTimers {
    timer =>
      timer.startSingleTimer("receive-connack", ReceiveConnAckTimeout, data.settings.receiveConnAckTimeout)

      Behaviors
        .receivePartial[Event] {
          case (context, ConnAckReceivedFromRemote(connAck, local))
              if connAck.returnCode.contains(ConnAckReturnCode.ConnectionAccepted) =>
            local.success(ForwardConnAck(data.connectData))
            if (data.connect.connectFlags.contains(ConnectFlags.CleanSession))
              context.children.foreach(context.stop)
            data.stash.foreach(context.self.tell)
            serverConnected(
              ConnAckReceived(
                data.connect.connectFlags,
                data.connect.keepAlive,
                pendingPingResp = false,
                Set.empty,
                Set.empty,
                Vector.empty,
                Vector.empty,
                data.remote,
                Vector.empty,
                data.consumerPacketRouter,
                data.producerPacketRouter,
                data.subscriberPacketRouter,
                data.unsubscriberPacketRouter,
                data.settings
              )
            )
          case (context, ConnAckReceivedFromRemote(_, local)) =>
            local.success(ForwardConnAck(data.connectData))
            disconnect(context, data.connect.connectFlags, data.remote, data)
          case (context, ReceiveConnAckTimeout) =>
            disconnect(context, data.connect.connectFlags, data.remote, data)
          case (context, ConnectionLost) =>
            disconnect(context, data.connect.connectFlags, data.remote, data)
          case (_, e) =>
            serverConnect(data.copy(stash = data.stash :+ e))
        }
        .receiveSignal {
          case (_, PostStop) =>
            data.remote.complete()
            Behaviors.same
        }

  }

  def serverConnected(data: ConnAckReceived)(implicit mat: Materializer): Behavior[Event] = Behaviors.withTimers {
    timer =>
      if (data.keepAlive.toMillis > 0)
        timer.startSingleTimer("send-pingreq", SendPingReqTimeout, data.keepAlive)
      else
        data.remote.complete() // We'll never be sending pings so free up the command channel for other things

      Behaviors
        .receivePartial[Event] {
          case (context, ConnectionLost) =>
            disconnect(context, data.connectFlags, data.remote, data)
          case (context, DisconnectReceivedLocally(remote)) =>
            remote.success(ForwardDisconnect)
            disconnect(context, data.connectFlags, data.remote, data)
          case (context, SubscribeReceivedLocally(subscribe, subscribeData, remote)) =>
            val subscriberName = ActorName.mkName(SubscriberNamePrefix + subscribe.topicFilters.map(_._1).mkString("-"))
            context.child(subscriberName) match {
              case None =>
                val subscriber = context.spawn(
                  Subscriber(subscribeData, remote, data.subscriberPacketRouter, data.settings),
                  subscriberName
                )
                context.watch(subscriber)
                pendingSubAck(
                  PendingSubscribe(
                    data.connectFlags,
                    data.keepAlive,
                    data.pendingPingResp,
                    data.activeConsumers,
                    data.activeProducers,
                    data.pendingLocalPublications,
                    data.pendingRemotePublications,
                    data.remote,
                    Vector.empty,
                    data.consumerPacketRouter,
                    data.producerPacketRouter,
                    data.subscriberPacketRouter,
                    data.unsubscriberPacketRouter,
                    data.settings
                  )
                )
              case _: Some[_] =>
                remote
                  .failure(new IllegalStateException("Shouldn't be able to receive subscriptions here: " + subscribe))
                serverConnected(data)
            }
          case (context, UnsubscribeReceivedLocally(unsubscribe, unsubscribeData, remote)) =>
            val unsubscriberName = ActorName.mkName(UnsubscriberNamePrefix + unsubscribe.topicFilters.mkString("-"))
            context.child(unsubscriberName) match {
              case None =>
                context.spawn(
                  Unsubscriber(unsubscribeData, remote, data.unsubscriberPacketRouter, data.settings),
                  unsubscriberName
                )
              case _: Some[_] =>
                remote.failure(new IllegalStateException("Duplicate unsubscribe: " + unsubscribe))
            }
            serverConnected(data)
          case (_, PublishReceivedFromRemote(publish, local))
              if (publish.flags & ControlPacketFlags.QoSReserved).underlying == 0 =>
            local.success(Consumer.ForwardPublish)
            serverConnected(data)
          case (context, prfr @ PublishReceivedFromRemote(publish @ Publish(_, topicName, Some(packetId), _), local)) =>
            if (!data.activeConsumers.contains(topicName)) {
              val consumerName = ActorName.mkName(ConsumerNamePrefix + topicName + "-" + context.children.size)
              context.watchWith(
                context.spawn(
                  Consumer(publish, None, packetId, local, data.consumerPacketRouter, data.settings),
                  consumerName
                ),
                ConsumerFree(publish.topicName)
              )
              serverConnected(data.copy(activeConsumers = data.activeConsumers + publish.topicName))
            } else {
              serverConnected(
                data.copy(pendingRemotePublications = data.pendingRemotePublications :+ (publish.topicName -> prfr))
              )
            }
          case (context, ConsumerFree(topicName)) =>
            val i = data.pendingRemotePublications.indexWhere(_._1 == topicName)
            if (i >= 0) {
              val prfr = data.pendingRemotePublications(i)._2
              val consumerName = ActorName.mkName(ConsumerNamePrefix + topicName + "-" + context.children.size)
              context.watchWith(
                context.spawn(
                  Consumer(prfr.publish,
                           None,
                           prfr.publish.packetId.get,
                           prfr.local,
                           data.consumerPacketRouter,
                           data.settings),
                  consumerName
                ),
                ConsumerFree(topicName)
              )
              serverConnected(
                data.copy(
                  pendingRemotePublications =
                  data.pendingRemotePublications.take(i) ++ data.pendingRemotePublications.drop(i + 1)
                )
              )
            } else {
              serverConnected(data.copy(activeConsumers = data.activeConsumers - topicName))
            }
          case (_, PublishReceivedLocally(publish, _))
              if (publish.flags & ControlPacketFlags.QoSReserved).underlying == 0 =>
            data.remote.offer(ForwardPublish(publish, None))
            serverConnected(data)
          case (context, prl @ PublishReceivedLocally(publish, publishData)) =>
            val producerName = ActorName.mkName(ProducerNamePrefix + publish.topicName + "-" + context.children.size)
            if (!data.activeProducers.contains(publish.topicName)) {
              val reply = Promise[Source[Producer.ForwardPublishingCommand, NotUsed]]
              import context.executionContext
              reply.future.foreach(command => context.self ! ReceivedProducerPublishingCommand(command))
              context.watchWith(
                context.spawn(Producer(publish, publishData, reply, data.producerPacketRouter, data.settings),
                              producerName),
                ProducerFree(publish.topicName)
              )
              serverConnected(data.copy(activeProducers = data.activeProducers + publish.topicName))
            } else {
              serverConnected(
                data.copy(pendingLocalPublications = data.pendingLocalPublications :+ (publish.topicName -> prl))
              )
            }
          case (context, ProducerFree(topicName)) =>
            val i = data.pendingLocalPublications.indexWhere(_._1 == topicName)
            if (i >= 0) {
              val prl = data.pendingLocalPublications(i)._2
              val producerName = ActorName.mkName(ProducerNamePrefix + topicName + "-" + context.children.size)
              val reply = Promise[Source[Producer.ForwardPublishingCommand, NotUsed]]
              import context.executionContext
              reply.future.foreach(command => context.self ! ReceivedProducerPublishingCommand(command))
              context.watchWith(
                context.spawn(
                  Producer(prl.publish, prl.publishData, reply, data.producerPacketRouter, data.settings),
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
              serverConnected(data.copy(activeProducers = data.activeProducers - topicName))
            }
          case (_, ReceivedProducerPublishingCommand(command)) =>
            command.runWith(Sink.foreach {
              case Producer.ForwardPublish(publish, packetId) => data.remote.offer(ForwardPublish(publish, packetId))
              case Producer.ForwardPubRel(_, packetId) => data.remote.offer(ForwardPubRel(packetId))
            })
            Behaviors.same
          case (context, SendPingReqTimeout) if data.pendingPingResp =>
            data.remote.fail(PingFailed)
            disconnect(context, data.connectFlags, data.remote, data)
          case (_, SendPingReqTimeout) =>
            data.remote.offer(ForwardPingReq)
            serverConnected(data.copy(pendingPingResp = true))
          case (_, PingRespReceivedFromRemote(local)) =>
            local.success(ForwardPingResp)
            serverConnected(data.copy(pendingPingResp = false))
        }
        .receiveSignal {
          case (_, _: Terminated) =>
            Behaviors.same
          case (_, PostStop) =>
            data.remote.complete()
            Behaviors.same
        }
  }

  def pendingSubAck(data: PendingSubscribe)(implicit mat: Materializer): Behavior[Event] =
    Behaviors
      .receivePartial[Event] {
        case (_, e) =>
          pendingSubAck(data.copy(stash = data.stash :+ e))
      }
      .receiveSignal {
        case (context, _: Terminated) =>
          data.stash.foreach(context.self.tell)
          serverConnected(
            ConnAckReceived(
              data.connectFlags,
              data.keepAlive,
              data.pendingPingResp,
              data.activeConsumers,
              data.activeProducers,
              data.pendingLocalPublications,
              data.pendingRemotePublications,
              data.remote,
              Vector.empty,
              data.consumerPacketRouter,
              data.producerPacketRouter,
              data.subscriberPacketRouter,
              data.unsubscriberPacketRouter,
              data.settings
            )
          )
        case (_, PostStop) =>
          data.remote.complete()
          Behaviors.same
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
   * No ACK received - the subscription failed
   */
  case object SubscribeFailed extends Exception with NoStackTrace

  /*
   * Construct with the starting state
   */
  def apply(subscribeData: SubscribeData,
            remote: Promise[ForwardSubscribe],
            packetRouter: ActorRef[LocalPacketRouter.Request[Event]],
            settings: MqttSessionSettings): Behavior[Event] =
    prepareServerSubscribe(Start(subscribeData, remote, packetRouter, settings))

  // Our FSM data, FSM events and commands emitted by the FSM

  sealed abstract class Data(val settings: MqttSessionSettings)
  final case class Start(subscribeData: SubscribeData,
                         remote: Promise[ForwardSubscribe],
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
  final case class SubAckReceivedFromRemote(local: Promise[ForwardSubAck]) extends Event
  case object ReceiveSubAckTimeout extends Event

  sealed abstract class Command
  final case class ForwardSubscribe(packetId: PacketId) extends Command
  final case class ForwardSubAck(connectData: SubscribeData) extends Command

  // State event handling

  def prepareServerSubscribe(data: Start): Behavior[Event] = Behaviors.setup { context =>
    val reply = Promise[LocalPacketRouter.Registered]
    data.packetRouter ! LocalPacketRouter.Register(context.self, reply)
    import context.executionContext
    reply.future.onComplete {
      case Success(registered: LocalPacketRouter.Registered) => context.self ! AcquiredPacketId(registered.packetId)
      case Failure(_) => context.self ! UnobtainablePacketId
    }

    Behaviors.receiveMessagePartial[Event] {
      case AcquiredPacketId(packetId) =>
        data.remote.success(ForwardSubscribe(packetId))
        serverSubscribe(
          ServerSubscribe(packetId, data.subscribeData, data.packetRouter, data.settings)
        )
      case UnobtainablePacketId =>
        data.remote.failure(SubscribeFailed)
        throw SubscribeFailed
    }
  }

  def serverSubscribe(data: ServerSubscribe): Behavior[Event] = Behaviors.withTimers { timer =>
    timer.startSingleTimer("receive-suback", ReceiveSubAckTimeout, data.settings.receiveSubAckTimeout)

    Behaviors
      .receiveMessagePartial[Event] {
        case SubAckReceivedFromRemote(local) =>
          local.success(ForwardSubAck(data.subscribeData))
          Behaviors.stopped
        case ReceiveSubAckTimeout =>
          throw SubscribeFailed
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

  /*
   * No ACK received - the unsubscription failed
   */
  case object UnsubscribeFailed extends Exception with NoStackTrace

  type UnsubscribeData = Option[_]

  /*
   * Construct with the starting state
   */
  def apply(unsubscribeData: UnsubscribeData,
            remote: Promise[ForwardUnsubscribe],
            packetRouter: ActorRef[LocalPacketRouter.Request[Event]],
            settings: MqttSessionSettings): Behavior[Event] =
    prepareServerUnsubscribe(Start(unsubscribeData, remote, packetRouter, settings))

  // Our FSM data, FSM events and commands emitted by the FSM

  sealed abstract class Data(val settings: MqttSessionSettings)
  final case class Start(unsubscribeData: UnsubscribeData,
                         remote: Promise[ForwardUnsubscribe],
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
  final case class UnsubAckReceivedFromRemote(local: Promise[ForwardUnsubAck]) extends Event
  case object ReceiveUnsubAckTimeout extends Event

  sealed abstract class Command
  final case class ForwardUnsubscribe(packetId: PacketId) extends Command
  final case class ForwardUnsubAck(connectData: UnsubscribeData) extends Command

  // State event handling

  def prepareServerUnsubscribe(data: Start): Behavior[Event] = Behaviors.setup { context =>
    val reply = Promise[LocalPacketRouter.Registered]
    data.packetRouter ! LocalPacketRouter.Register(context.self, reply)
    import context.executionContext
    reply.future.onComplete {
      case Success(registered: LocalPacketRouter.Registered) => context.self ! AcquiredPacketId(registered.packetId)
      case Failure(_) => context.self ! UnobtainablePacketId
    }

    Behaviors.receiveMessagePartial[Event] {
      case AcquiredPacketId(packetId) =>
        data.remote.success(ForwardUnsubscribe(packetId))
        serverUnsubscribe(
          ServerUnsubscribe(packetId, data.unsubscribeData, data.packetRouter, data.settings)
        )
      case UnobtainablePacketId =>
        data.remote.failure(UnsubscribeFailed)
        throw UnsubscribeFailed
    }
  }

  def serverUnsubscribe(data: ServerUnsubscribe): Behavior[Event] = Behaviors.withTimers { timer =>
    timer.startSingleTimer("receive-unsubAck", ReceiveUnsubAckTimeout, data.settings.receiveUnsubAckTimeout)

    Behaviors
      .receiveMessagePartial[Event] {
        case UnsubAckReceivedFromRemote(local) =>
          local.success(ForwardUnsubAck(data.unsubscribeData))
          Behaviors.stopped
        case ReceiveUnsubAckTimeout =>
          throw UnsubscribeFailed
      }
      .receiveSignal {
        case (_, PostStop) =>
          data.packetRouter ! LocalPacketRouter.Unregister(data.packetId)
          Behaviors.same
      }
  }
}
