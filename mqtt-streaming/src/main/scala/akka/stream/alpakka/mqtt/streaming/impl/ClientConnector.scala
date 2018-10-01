/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.mqtt.streaming
package impl

import akka.actor.typed.{ActorRef, Behavior}
import akka.actor.typed.scaladsl.Behaviors
import akka.annotation.InternalApi
import akka.util.Timeout

import scala.util.{Failure, Success}

/*
 * A client connector is a Finite State Machine that manages MQTT client
 * session state. A client connects to a server, subscribes/unsubscribes
 * from topics to receive publications on and publishes to its own topics.
 */
@InternalApi private[streaming] object ClientConnector {

  type ConnectData = Option[_]

  /*
   * Construct with the starting state
   */
  def apply(packetIdAllocator: ActorRef[PacketIdAllocator.Request], settings: MqttSessionSettings): Behavior[Event] =
    disconnected(Uninitialized(packetIdAllocator, settings))

  // Our FSM data, FSM events and commands emitted by the FSM

  sealed abstract class Data(val packetIdAllocator: ActorRef[PacketIdAllocator.Request],
                             val settings: MqttSessionSettings)
  final case class Uninitialized(override val packetIdAllocator: ActorRef[PacketIdAllocator.Request],
                                 override val settings: MqttSessionSettings)
      extends Data(packetIdAllocator, settings)
  final case class ConnectReceived(connect: Connect,
                                   connectData: ConnectData,
                                   override val packetIdAllocator: ActorRef[PacketIdAllocator.Request],
                                   override val settings: MqttSessionSettings)
      extends Data(packetIdAllocator, settings)
  final case class ConnAckReceived(connect: Connect,
                                   connAck: ConnAck,
                                   override val packetIdAllocator: ActorRef[PacketIdAllocator.Request],
                                   override val settings: MqttSessionSettings)
      extends Data(packetIdAllocator, settings)

  sealed abstract class Event
  final case class ConnectReceivedLocally(connect: Connect,
                                          connectData: ConnectData,
                                          remote: ActorRef[ForwardConnect.type])
      extends Event
  final case class ConnAckReceivedFromRemote(connAck: ConnAck, local: ActorRef[ForwardConnAck]) extends Event
  case object ReceiveConnAckTimeout extends Event
  case object ConnectionLost extends Event
  final case class DisconnectReceivedLocally(remote: ActorRef[ForwardDisconnect.type]) extends Event
  final case class SubscribeReceivedLocally(subscribe: Subscribe,
                                            subscribeData: Subscriber.SubscribeData,
                                            remote: ActorRef[Subscriber.ForwardSubscribe])
      extends Event
  final case class SubAckReceivedFromRemote(subAck: SubAck, local: ActorRef[Subscriber.ForwardSubAck]) extends Event

  sealed abstract class Command
  case object ForwardConnect extends Command
  final case class ForwardConnAck(connectData: ConnectData) extends Command
  case object ForwardDisconnect extends Command

  // State event handling

  def disconnected(data: Uninitialized): Behavior[Event] = Behaviors.receiveMessagePartial {
    case ConnectReceivedLocally(connect, connectData, remote) =>
      remote ! ForwardConnect
      serverConnect(ConnectReceived(connect, connectData, data.packetIdAllocator, data.settings))
  }

  def serverConnect(data: ConnectReceived): Behavior[Event] = Behaviors.withTimers { timer =>
    timer.startSingleTimer("receive-connack", ReceiveConnAckTimeout, data.settings.receiveConnAckTimeout)
    Behaviors.receiveMessagePartial {
      case ConnAckReceivedFromRemote(connAck, local)
          if connAck.returnCode.contains(ConnAckReturnCode.ConnectionAccepted) =>
        local ! ForwardConnAck(data.connectData)
        if (data.connect.connectFlags.contains(ConnectFlags.CleanSession)) cleanSession()
        serverConnected(ConnAckReceived(data.connect, connAck, data.packetIdAllocator, data.settings))
      case ConnAckReceivedFromRemote(_, local) =>
        local ! ForwardConnAck(data.connectData)
        disconnected(Uninitialized(data.packetIdAllocator, data.settings))
      case ReceiveConnAckTimeout =>
        disconnected(Uninitialized(data.packetIdAllocator, data.settings))
    }
  }

  def serverConnected(data: ConnAckReceived): Behavior[Event] = Behaviors.receivePartial {
    case (_, ConnectionLost) =>
      disconnected(Uninitialized(data.packetIdAllocator, data.settings))
    case (_, DisconnectReceivedLocally(remote)) =>
      remote ! ForwardDisconnect
      cleanSession()
      disconnected(Uninitialized(data.packetIdAllocator, data.settings))
    case (context, SubscribeReceivedLocally(subscribe, subscribeData, remote)) =>
      subscribe.topicFilters.foreach { topicFilter =>
        val (topicName, _) = topicFilter
        context.spawn(Subscriber(subscribeData, remote, data.packetIdAllocator, data.settings), "topic-" + topicName) // FIXME: How about existing children? getOrElse...?
      }
      Behaviors.same
    case (context, SubAckReceivedFromRemote(subAck, local)) =>
      context.children.foreach {
        case child: ActorRef[Subscriber.Event] @unchecked =>
          child ! Subscriber
            .SubAckReceivedFromRemote(subAck, local) // FIXME: We should probably watch our own subscribers as there will be other types of child actor
      }
      Behaviors.same
    // TODO: UnsubscribedReceivedLocally, SubscribeReceivedLocally, PublishReceivedFromRemote, PubRelReceivedFromRemote, PubAckReceivedLocally, PubRecReceivedLocally, PubCompReceivedLocally, PublishReceivedLocally, PubRecReceivedLocally, PubCompReceivedLocally, PubAckReceivedFromRemote, PubRelReceivedFromRemote
  }

  // Actions

  def cleanSession(): Unit = {} // TODO
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
            packetIdAllocator: ActorRef[PacketIdAllocator.Request],
            settings: MqttSessionSettings): Behavior[Event] =
    prepareServerSubscribe(Start(subscribeData, remote, packetIdAllocator, settings))

  // Our FSM data, FSM events and commands emitted by the FSM

  sealed abstract class Data(val settings: MqttSessionSettings)
  final case class Start(subscribeData: SubscribeData,
                         remote: ActorRef[ForwardSubscribe],
                         packetIdAllocator: ActorRef[PacketIdAllocator.Request],
                         override val settings: MqttSessionSettings)
      extends Data(settings)
  final case class ServerSubscribe(packetId: PacketId,
                                   subscribeData: SubscribeData,
                                   packetIdAllocator: ActorRef[PacketIdAllocator.Request],
                                   override val settings: MqttSessionSettings)
      extends Data(settings)
  final case class ServerSubscribed(override val settings: MqttSessionSettings) extends Data(settings)

  sealed abstract class Event
  final case class AcquiredPacketId(packetId: PacketId) extends Event
  final case object UnacquiredPacketId extends Event
  final case class SubAckReceivedFromRemote(subAck: SubAck, local: ActorRef[ForwardSubAck]) extends Event
  case object ReceiveSubAckTimeout extends Event

  sealed abstract class Command
  case class ForwardSubscribe(packetId: PacketId) extends Command
  final case class ForwardSubAck(connectData: SubscribeData) extends Command

  // State event handling

  def prepareServerSubscribe(data: Start): Behavior[Event] = Behaviors.setup { context =>
    implicit val actorMqttSessionTimeout: Timeout = data.settings.actorMqttSessionTimeout

    context.ask(data.packetIdAllocator)(PacketIdAllocator.Acquire) {
      case Success(acquired: PacketIdAllocator.Acquired) => AcquiredPacketId(acquired.packetId)
      case Failure(_) => UnacquiredPacketId
    }

    Behaviors.receiveMessagePartial {
      case AcquiredPacketId(packetId) =>
        data.remote ! ForwardSubscribe(packetId)
        serverSubscribe(ServerSubscribe(packetId, data.subscribeData, data.packetIdAllocator, data.settings))
      case UnacquiredPacketId =>
        Behaviors.stopped
    }
  }

  def serverSubscribe(data: ServerSubscribe): Behavior[Event] = Behaviors.withTimers { timer =>
    timer.startSingleTimer("receive-suback", ReceiveSubAckTimeout, data.settings.receiveSubAckTimeout)

    Behaviors.receiveMessagePartial {
      case SubAckReceivedFromRemote(subAck, local) if subAck.packetId == data.packetId =>
        local ! ForwardSubAck(data.subscribeData)
        data.packetIdAllocator ! PacketIdAllocator.Release(data.packetId)
        serverSubscribed(ServerSubscribed(data.settings))
      case SubAckReceivedFromRemote(subAck, local) =>
        Behaviors.same // Ignore sub acks not destined for us
      case ReceiveSubAckTimeout =>
        data.packetIdAllocator ! PacketIdAllocator.Release(data.packetId)
        Behaviors.stopped
    }
  }

  def serverSubscribed(data: ServerSubscribed): Behavior[Event] = Behaviors.same // TODO: This is where I'm up to.
}

/*
 * Manage packet identifiers for MQTT. Once released, they can
 * be re-used. The algorithm is optimised to return allocated
 * packet ids fast, and take the cost when releasing them as
 * the client isn't waiting on a reply.
 */
@InternalApi private[streaming] object PacketIdAllocator {

  /*
   * In case some brokers treat 0 as no packet id, we set our min to 1
   * e.g. https://renesasrulz.com/synergy/synergy_tech_notes/f/technical-bulletin-board-notification-postings/8998/mqtt-client-packet-identifier-is-0-by-default-which-causes-azure-iot-hub-to-reset-connection
   */
  private val MinPacketId = PacketId(1)
  private val MaxPacketId = PacketId(0xffff)

  /*
   * Construct with the starting state
   */
  def apply(): Behavior[Request] =
    main(Set.empty, MinPacketId)

  // Requests

  sealed abstract class Request
  final case class Acquire(replyTo: ActorRef[Acquired]) extends Request
  final case class Release(packetId: PacketId) extends Request

  // Replies

  sealed abstract class Reply
  final case class Acquired(packetId: PacketId) extends Reply

  // Processing

  def main(allocatedPacketIds: Set[PacketId], nextPacketId: PacketId): Behavior[Request] = Behaviors.receiveMessage {
    case Acquire(replyTo) if nextPacketId.underlying <= MaxPacketId.underlying =>
      replyTo ! Acquired(nextPacketId)
      main(allocatedPacketIds + nextPacketId, PacketId(nextPacketId.underlying + 1))
    case _: Acquire =>
      Behaviors.same // We cannot allocate any more. This will eventually cause a timeout to occur on the requestor.
    case Release(packetId) =>
      val remainingPacketIds = allocatedPacketIds - packetId
      val revisedNextPacketId = if (remainingPacketIds.nonEmpty) {
        val maxPacketId = PacketId(remainingPacketIds.map(_.underlying).max)
        PacketId(maxPacketId.underlying + 1)
      } else {
        MinPacketId
      }
      main(remainingPacketIds, revisedNextPacketId)
  }

}
