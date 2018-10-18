/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.mqtt.streaming
package impl

import akka.NotUsed
import akka.actor.typed.{ActorRef, Behavior, PostStop}
import akka.actor.typed.scaladsl.Behaviors
import akka.annotation.InternalApi
import akka.stream.{Materializer, OverflowStrategy}
import akka.stream.scaladsl.{BroadcastHub, Keep, Source, SourceQueueWithComplete}
import akka.stream.typed.scaladsl.ActorMaterializer
import akka.util.Timeout

import scala.util.control.NoStackTrace
import scala.util.{Failure, Success}

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
  def apply(publish: Publish,
            publishData: PublishData,
            remote: ActorRef[Source[ForwardPublishingCommand, NotUsed]],
            packetRouter: ActorRef[LocalPacketRouter.Request[Event]],
            settings: MqttSessionSettings): Behavior[Event] =
    preparePublish(Start(publish, publishData, remote, packetRouter, settings))

  // Our FSM data, FSM events and commands emitted by the FSM

  sealed abstract class Data(val publish: Publish, val publishData: PublishData, val settings: MqttSessionSettings)
  final case class Start(override val publish: Publish,
                         override val publishData: PublishData,
                         remote: ActorRef[Source[ForwardPublishingCommand, NotUsed]],
                         packetRouter: ActorRef[LocalPacketRouter.Request[Event]],
                         override val settings: MqttSessionSettings)
      extends Data(publish, publishData, settings)
  final case class Publishing(remote: SourceQueueWithComplete[ForwardPublishingCommand],
                              packetId: PacketId,
                              override val publish: Publish,
                              override val publishData: PublishData,
                              packetRouter: ActorRef[LocalPacketRouter.Request[Event]],
                              override val settings: MqttSessionSettings)
      extends Data(publish, publishData, settings)

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
  final case class ForwardPublish(publish: Publish, packetId: Option[PacketId]) extends ForwardPublishingCommand
  final case class ForwardPubAck(publishData: PublishData) extends Command
  final case class ForwardPubRec(publishData: PublishData) extends Command
  final case class ForwardPubRel(publish: Publish, packetId: PacketId) extends ForwardPublishingCommand
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
          queue.offer(ForwardPublish(data.publish, Some(packetId)))
          publishUnacknowledged(
            Publishing(queue, packetId, data.publish, data.publishData, data.packetRouter, data.settings)
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
        case PubAckReceivedFromRemote(local)
            if data.publish.flags.contains(ControlPacketFlags.QoSAtLeastOnceDelivery) =>
          local ! ForwardPubAck(data.publishData)
          Behaviors.stopped
        case PubRecReceivedFromRemote(local) if data.publish.flags.contains(ControlPacketFlags.QoSAtMostOnceDelivery) =>
          local ! ForwardPubRec(data.publishData)
          publishAcknowledged(data)
        case ReceivePubAckRecTimeout =>
          data.remote.offer(
            ForwardPublish(data.publish.copy(flags = data.publish.flags | ControlPacketFlags.DUP), Some(data.packetId))
          )
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

    data.remote.offer(ForwardPubRel(data.publish, data.packetId))

    Behaviors
      .receiveMessagePartial[Event] {
        case PubCompReceivedFromRemote(local) =>
          local ! ForwardPubComp(data.publishData)
          Behaviors.stopped
        case ReceivePubCompTimeout =>
          data.remote.offer(ForwardPubRel(data.publish, data.packetId))
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
   * No ACK received - the publication failed
   */
  case object ConsumeFailed extends Exception with NoStackTrace

  /*
   * Construct with the starting state
   */
  def apply(publish: Publish,
            packetId: PacketId,
            local: ActorRef[ForwardPublish.type],
            packetRouter: ActorRef[RemotePacketRouter.Request[Event]],
            settings: MqttSessionSettings): Behavior[Event] =
    prepareClientConsumption(Start(publish, packetId, local, packetRouter, settings))

  // Our FSM data, FSM events and commands emitted by the FSM

  sealed abstract class Data(val publish: Publish,
                             val packetId: PacketId,
                             val packetRouter: ActorRef[RemotePacketRouter.Request[Event]],
                             val settings: MqttSessionSettings)
  final case class Start(override val publish: Publish,
                         override val packetId: PacketId,
                         local: ActorRef[ForwardPublish.type],
                         override val packetRouter: ActorRef[RemotePacketRouter.Request[Event]],
                         override val settings: MqttSessionSettings)
      extends Data(publish, packetId, packetRouter, settings)
  final case class ClientConsuming(override val publish: Publish,
                                   override val packetId: PacketId,
                                   override val packetRouter: ActorRef[RemotePacketRouter.Request[Event]],
                                   override val settings: MqttSessionSettings)
      extends Data(publish, packetId, packetRouter, settings)

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

    Behaviors.receiveMessagePartial[Event] {
      case RegisteredPacketId =>
        data.local ! ForwardPublish
        consumeUnacknowledged(ClientConsuming(data.publish, data.packetId, data.packetRouter, data.settings))
      case UnobtainablePacketId =>
        throw ConsumeFailed
    }

  }

  def consumeUnacknowledged(data: ClientConsuming): Behavior[Event] = Behaviors.withTimers { timer =>
    timer.startSingleTimer("receive-pubackrel", ReceivePubAckRecTimeout, data.settings.receivePubAckRecTimeout)
    Behaviors
      .receiveMessagePartial[Event] {
        case PubAckReceivedLocally(remote) if data.publish.flags.contains(ControlPacketFlags.QoSAtLeastOnceDelivery) =>
          remote ! ForwardPubAck
          Behaviors.stopped
        case PubRecReceivedLocally(remote) if data.publish.flags.contains(ControlPacketFlags.QoSExactlyOnceDelivery) =>
          remote ! ForwardPubRec
          consumeReceived(data)
        case ReceivePubAckRecTimeout =>
          throw ConsumeFailed
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
          throw ConsumeFailed
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
        case ReceivePubCompTimeout =>
          throw ConsumeFailed
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
