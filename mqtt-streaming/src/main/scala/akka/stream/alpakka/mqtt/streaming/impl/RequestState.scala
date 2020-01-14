/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.mqtt.streaming
package impl

import akka.NotUsed
import akka.actor.typed.{ActorRef, Behavior, PostStop}
import akka.actor.typed.scaladsl.Behaviors
import akka.annotation.InternalApi
import akka.stream.{Materializer, OverflowStrategy, QueueOfferResult}
import akka.stream.scaladsl.{BroadcastHub, Keep, Source, SourceQueueWithComplete}
import akka.util.ByteString

import scala.annotation.tailrec
import scala.concurrent.Promise
import scala.util.control.NoStackTrace
import scala.util.{Either, Failure, Success}

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
            remote: Promise[Source[ForwardPublishingCommand, NotUsed]],
            packetRouter: ActorRef[LocalPacketRouter.Request[Event]],
            settings: MqttSessionSettings)(implicit mat: Materializer): Behavior[Event] =
    preparePublish(Start(publish, publishData, remote, packetRouter, settings))

  // Our FSM data, FSM events and commands emitted by the FSM

  sealed abstract class Data(val publish: Publish, val publishData: PublishData, val settings: MqttSessionSettings)
  final case class Start(override val publish: Publish,
                         override val publishData: PublishData,
                         remote: Promise[Source[ForwardPublishingCommand, NotUsed]],
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
  final case class PubAckReceivedFromRemote(local: Promise[ForwardPubAck]) extends Event
  final case class PubRecReceivedFromRemote(local: Promise[ForwardPubRec]) extends Event
  case object ReceivePubCompTimeout extends Event
  final case class PubCompReceivedFromRemote(local: Promise[ForwardPubComp]) extends Event
  case object ReceiveConnect extends Event

  final case class QueueOfferCompleted(result: Either[Throwable, QueueOfferResult])
      extends Event
      with QueueOfferState.QueueOfferCompleted

  sealed abstract class Command
  sealed abstract class ForwardPublishingCommand extends Command
  final case class ForwardPublish(publish: Publish, packetId: Option[PacketId]) extends ForwardPublishingCommand
  final case class ForwardPubAck(publishData: PublishData) extends Command
  final case class ForwardPubRec(publishData: PublishData) extends Command
  final case class ForwardPubRel(publish: Publish, packetId: PacketId) extends ForwardPublishingCommand
  final case class ForwardPubComp(publishData: PublishData) extends Command

  // State event handling

  def preparePublish(data: Start)(implicit mat: Materializer): Behavior[Event] = Behaviors.setup { context =>
    def requestPacketId(): Unit = {
      val reply = Promise[LocalPacketRouter.Registered]
      data.packetRouter ! LocalPacketRouter.Register(context.self.unsafeUpcast, reply)
      import context.executionContext
      reply.future.onComplete {
        case Success(acquired: LocalPacketRouter.Registered) => context.self ! AcquiredPacketId(acquired.packetId)
        case Failure(_) => context.self ! UnacquiredPacketId
      }
    }

    requestPacketId()

    val (queue, source) = Source
      .queue[ForwardPublishingCommand](data.settings.clientSendBufferSize, OverflowStrategy.backpressure)
      .toMat(BroadcastHub.sink)(Keep.both)
      .run()

    data.remote.success(source)

    Behaviors
      .receiveMessagePartial[Event] {
        case AcquiredPacketId(packetId) =>
          QueueOfferState.waitForQueueOfferCompleted(
            queue
              .offer(ForwardPublish(data.publish, Some(packetId))),
            result => QueueOfferCompleted(result.toEither),
            publishUnacknowledged(
              Publishing(queue, packetId, data.publish, data.publishData, data.packetRouter, data.settings)
            ),
            stash = Vector.empty
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

  def publishUnacknowledged(data: Publishing)(implicit mat: Materializer): Behavior[Event] = Behaviors.withTimers {
    val ReceivePubackrec = "producer-receive-pubackrec"
    timer =>
      if (data.settings.producerPubAckRecTimeout.toNanos > 0L)
        timer.startSingleTimer(ReceivePubackrec, ReceivePubAckRecTimeout, data.settings.producerPubAckRecTimeout)

      Behaviors
        .receive[Event] {
          case (_, PubAckReceivedFromRemote(local))
              if data.publish.flags.contains(ControlPacketFlags.QoSAtLeastOnceDelivery) =>
            local.success(ForwardPubAck(data.publishData))
            Behaviors.stopped

          case (_, PubRecReceivedFromRemote(local))
              if data.publish.flags.contains(ControlPacketFlags.QoSAtMostOnceDelivery) =>
            local.success(ForwardPubRec(data.publishData))
            timer.cancel(ReceivePubackrec)
            publishAcknowledged(data)

          case (context, ReceivePubAckRecTimeout | ReceiveConnect) =>
            QueueOfferState.waitForQueueOfferCompleted(
              data.remote
                .offer(
                  ForwardPublish(data.publish.copy(flags = data.publish.flags | ControlPacketFlags.DUP),
                                 Some(data.packetId))
                ),
              result => QueueOfferCompleted(result.toEither),
              publishUnacknowledged(data),
              stash = Vector.empty
            )
        }
        .receiveSignal {
          case (_, PostStop) =>
            data.remote.complete()
            Behaviors.same
        }
  }

  def publishAcknowledged(data: Publishing)(implicit mat: Materializer): Behavior[Event] = Behaviors.withTimers {
    val ReceivePubrel = "producer-receive-pubrel"
    timer =>
      if (data.settings.producerPubCompTimeout.toNanos > 0L)
        timer.startSingleTimer(ReceivePubrel, ReceivePubCompTimeout, data.settings.producerPubCompTimeout)

      Behaviors.setup { context =>
        QueueOfferState.waitForQueueOfferCompleted(
          data.remote
            .offer(ForwardPubRel(data.publish, data.packetId)),
          result => QueueOfferCompleted(result.toEither),
          Behaviors
            .receiveMessagePartial[Event] {
              case PubCompReceivedFromRemote(local) =>
                local.success(ForwardPubComp(data.publishData))
                Behaviors.stopped
              case ReceivePubCompTimeout | ReceiveConnect =>
                QueueOfferState.waitForQueueOfferCompleted(
                  data.remote
                    .offer(ForwardPubRel(data.publish, data.packetId)),
                  result => QueueOfferCompleted(result.toEither),
                  publishAcknowledged(data),
                  stash = Vector.empty
                )
            }
            .receiveSignal {
              case (_, PostStop) =>
                data.remote.complete()
                Behaviors.same
            },
          stash = Vector.empty
        )
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
  case class ConsumeFailed(publish: Publish) extends Exception(publish.toString) with NoStackTrace

  /*
   * Construct with the starting state
   */
  def apply(publish: Publish,
            clientId: Option[String],
            packetId: PacketId,
            local: Promise[ForwardPublish.type],
            packetRouter: ActorRef[RemotePacketRouter.Request[Event]],
            settings: MqttSessionSettings): Behavior[Event] =
    prepareClientConsumption(Start(publish, clientId, packetId, local, packetRouter, settings))

  // Our FSM data, FSM events and commands emitted by the FSM

  sealed abstract class Data(val publish: Publish,
                             val clientId: Option[String],
                             val packetId: PacketId,
                             val packetRouter: ActorRef[RemotePacketRouter.Request[Event]],
                             val settings: MqttSessionSettings)
  final case class Start(override val publish: Publish,
                         override val clientId: Option[String],
                         override val packetId: PacketId,
                         local: Promise[ForwardPublish.type],
                         override val packetRouter: ActorRef[RemotePacketRouter.Request[Event]],
                         override val settings: MqttSessionSettings)
      extends Data(publish, clientId, packetId, packetRouter, settings)
  final case class ClientConsuming(override val publish: Publish,
                                   override val clientId: Option[String],
                                   override val packetId: PacketId,
                                   override val packetRouter: ActorRef[RemotePacketRouter.Request[Event]],
                                   override val settings: MqttSessionSettings)
      extends Data(publish, clientId, packetId, packetRouter, settings)

  sealed abstract class Event
  final case object RegisteredPacketId extends Event
  final case object UnobtainablePacketId extends Event
  final case class PubAckReceivedLocally(remote: Promise[ForwardPubAck.type]) extends Event
  final case class PubRecReceivedLocally(remote: Promise[ForwardPubRec.type]) extends Event
  case object ReceivePubAckRecTimeout extends Event
  final case class PubRelReceivedFromRemote(local: Promise[ForwardPubRel.type]) extends Event
  case object ReceivePubRelTimeout extends Event
  final case class PubCompReceivedLocally(remote: Promise[ForwardPubComp.type]) extends Event
  case object ReceivePubCompTimeout extends Event
  final case class DupPublishReceivedFromRemote(local: Promise[ForwardPublish.type]) extends Event

  sealed abstract class Command
  case object ForwardPublish extends Command
  case object ForwardPubAck extends Command
  case object ForwardPubRec extends Command
  case object ForwardPubRel extends Command
  case object ForwardPubComp extends Command

  // State event handling

  def prepareClientConsumption(data: Start): Behavior[Event] = Behaviors.setup { context =>
    val reply = Promise[RemotePacketRouter.Registered.type]
    data.packetRouter ! RemotePacketRouter.Register(context.self.unsafeUpcast, data.clientId, data.packetId, reply)
    import context.executionContext
    reply.future.onComplete {
      case Success(RemotePacketRouter.Registered) => context.self ! RegisteredPacketId
      case Failure(_) => context.self ! UnobtainablePacketId
    }

    Behaviors.receiveMessagePartial[Event] {
      case RegisteredPacketId =>
        data.local.success(ForwardPublish)
        consumeUnacknowledged(
          ClientConsuming(data.publish, data.clientId, data.packetId, data.packetRouter, data.settings)
        )
      case UnobtainablePacketId =>
        val ex = ConsumeFailed(data.publish)
        data.local.failure(ex)
        throw ex
    }

  }

  def consumeUnacknowledged(data: ClientConsuming): Behavior[Event] = Behaviors.withTimers { timer =>
    val ReceivePubackrel = "consumer-receive-pubackrel"
    timer.startSingleTimer(ReceivePubackrel, ReceivePubAckRecTimeout, data.settings.consumerPubAckRecTimeout)
    Behaviors
      .receiveMessagePartial[Event] {
        case PubAckReceivedLocally(remote) if data.publish.flags.contains(ControlPacketFlags.QoSAtLeastOnceDelivery) =>
          remote.success(ForwardPubAck)
          Behaviors.stopped
        case PubRecReceivedLocally(remote) if data.publish.flags.contains(ControlPacketFlags.QoSExactlyOnceDelivery) =>
          remote.success(ForwardPubRec)
          timer.cancel(ReceivePubackrel)
          consumeReceived(data)
        case DupPublishReceivedFromRemote(local) =>
          local.success(ForwardPublish)
          consumeUnacknowledged(data)
        case ReceivePubAckRecTimeout =>
          throw ConsumeFailed(data.publish)
      }
  }

  def consumeReceived(data: ClientConsuming): Behavior[Event] = Behaviors.withTimers { timer =>
    val ReceivePubrel = "consumer-receive-pubrel"
    timer.startSingleTimer(ReceivePubrel, ReceivePubRelTimeout, data.settings.consumerPubRelTimeout)
    Behaviors
      .receiveMessagePartial[Event] {
        case PubRelReceivedFromRemote(local) =>
          local.success(ForwardPubRel)
          timer.cancel(ReceivePubrel)
          consumeAcknowledged(data)
        case DupPublishReceivedFromRemote(local) =>
          local.success(ForwardPublish)
          consumeUnacknowledged(data)
        case ReceivePubRelTimeout =>
          throw ConsumeFailed(data.publish)
      }
  }

  def consumeAcknowledged(data: ClientConsuming): Behavior[Event] = Behaviors.withTimers { timer =>
    val ReceivePubcomp = "consumer-receive-pubcomp"
    timer.startSingleTimer(ReceivePubcomp, ReceivePubCompTimeout, data.settings.consumerPubCompTimeout)
    Behaviors
      .receiveMessagePartial[Event] {
        case PubCompReceivedLocally(remote) =>
          remote.success(ForwardPubComp)
          Behaviors.stopped
        case DupPublishReceivedFromRemote(local) =>
          local.success(ForwardPublish)
          timer.cancel(ReceivePubcomp)
          consumeUnacknowledged(data)
        case ReceivePubCompTimeout =>
          throw ConsumeFailed(data.publish)
      }
  }
}

@InternalApi private[streaming] object LocalPacketRouter {
  /*
   * Raised on routing if a packet id cannot determine an actor to route to
   */
  case class CannotRoute(packetId: PacketId) extends Exception("packet id: " + packetId.underlying) with NoStackTrace

  /*
   * In case some brokers treat 0 as no packet id, we set our min to 1
   * e.g. https://renesasrulz.com/synergy/synergy_tech_notes/f/technical-bulletin-board-notification-postings/8998/mqtt-client-packet-identifier-is-0-by-default-which-causes-azure-iot-hub-to-reset-connection
   */
  val MinPacketId = PacketId(1)
  val MaxPacketId = PacketId(0xffff)

  // Requests

  sealed abstract class Request[A]
  final case class Register[A](registrant: ActorRef[A], reply: Promise[Registered]) extends Request[A]
  private final case class Unregister[A](packetId: PacketId) extends Request[A]
  final case class Route[A](packetId: PacketId, event: A, failureReply: Promise[_]) extends Request[A]

  // Replies

  sealed abstract class Reply
  final case class Registered(packetId: PacketId) extends Reply

  /*
   * Construct with the starting state
   */
  def apply[A]: Behavior[Request[A]] =
    new LocalPacketRouter[A].main(Map.empty, Some(MinPacketId), Vector.empty)

  /**
   * Find the next free packet id after the specified one.
   */
  def findNextPacketId[A](registrantsByPacketId: Map[PacketId, Registration[A]], after: PacketId): Option[PacketId] = {
    @annotation.tailrec
    def step(c: PacketId): Option[PacketId] = {
      if (c.underlying == after.underlying) {
        // this is a bug, given our guard for entry into `step` checks size. this
        // means an illegal packet was stored in the map
        throw new IllegalStateException("Cannot find a free packet id even though one is expected")
      }

      if (c.underlying <= MaxPacketId.underlying && !registrantsByPacketId.contains(c))
        Some(c)
      else if (c.underlying < MaxPacketId.underlying)
        step(PacketId(c.underlying + 1))
      else
        step(MinPacketId)
    }

    if (registrantsByPacketId.size == (MaxPacketId.underlying - MinPacketId.underlying))
      None
    else
      step(PacketId(after.underlying + 1))
  }

  private[streaming] case class Registration[A](registrant: ActorRef[A], failureReplies: Seq[Promise[_]])
}

/*
 * Route locally generated MQTT packets based on packet identifiers.
 * Callers are able to request that they be registered for routing and,
 * in return, receive the packet identifier acquired. These
 * callers are then watched for termination so that the packet identifier
 * can be released to become reused, and for any other housekeeping. The
 * contract is therefore for a caller to initially register, and to
 * terminate when it has finished with the packet identifier.
 *
 * The acquisition algorithm is optimised to return newly allocated
 * packet ids fast, and take the cost when releasing them as
 * the caller isn't waiting on a reply.
 */
@InternalApi private[streaming] final class LocalPacketRouter[A] {

  import LocalPacketRouter._

  // Processing

  def main(registrantsByPacketId: Map[PacketId, Registration[A]],
           nextPacketId: Option[PacketId],
           pendingRegistrations: Vector[Register[A]]): Behavior[Request[A]] =
    Behaviors
      .receive[Request[A]] {
        case (context, register @ Register(registrant: ActorRef[A], reply)) =>
          nextPacketId match {
            case Some(currentPacketId) =>
              reply.success(Registered(currentPacketId))

              val nextRegistrations = registrantsByPacketId + (currentPacketId -> Registration(registrant, List.empty))

              context.watchWith(registrant, Unregister(currentPacketId))

              main(
                nextRegistrations,
                findNextPacketId(nextRegistrations, currentPacketId),
                pendingRegistrations
              )

            case None =>
              // all packet ids are taken, so we'll wait until one is unregistered
              // to continue

              main(registrantsByPacketId, nextPacketId, pendingRegistrations :+ register)
          }

        case (context, Unregister(packetId)) =>
          // We tidy up and fail any failure promises that haven't already been failed -
          // just in case the registrant terminated abnormally and didn't get to complete
          // the promise. We all know that uncompleted promises can lead to memory leaks.
          // The known condition by which we'd succeed in failing the promise here is
          // when we thought we were able to route to a registrant, but the routing
          // subsequently failed, ending up the in the deadletter queue.
          registrantsByPacketId.get(packetId).toList.flatMap(_.failureReplies).foreach { failureReply =>
            failureReply.tryFailure(CannotRoute(packetId))
          }

          val remainingPacketIds = registrantsByPacketId - packetId

          pendingRegistrations
            .foreach(context.self.tell)

          main(remainingPacketIds, Some(nextPacketId.getOrElse(packetId)), Vector.empty)

        case (_, Route(packetId, event, failureReply)) =>
          registrantsByPacketId.get(packetId) match {
            case Some(registration: Registration[A]) =>
              registration.registrant ! event
              main(registrantsByPacketId
                     .updated(packetId,
                              registration.copy(failureReplies = failureReply +: registration.failureReplies)),
                   nextPacketId,
                   pendingRegistrations)
            case None =>
              failureReply.failure(CannotRoute(packetId))
              Behaviors.same
          }
      }
      .receiveSignal {
        case (_, PostStop) =>
          pendingRegistrations
            .foreach(_.reply.failure(new IllegalStateException("LocalPacketRouter was stopped")))

          Behaviors.stopped
      }
}

@InternalApi private[streaming] object RemotePacketRouter {
  /*
   * Raised on routing if a packet id cannot determine an actor to route to
   */
  case class CannotRoute(packetId: PacketId) extends Exception("packet id: " + packetId.underlying) with NoStackTrace

  // Requests

  sealed abstract class Request[A]
  final case class Register[A](registrant: ActorRef[A],
                               clientId: Option[String],
                               packetId: PacketId,
                               reply: Promise[Registered.type])
      extends Request[A]
  final case class RegisterConnection[A](connectionId: ByteString, clientId: String) extends Request[A]
  private final case class Unregister[A](clientId: Option[String], packetId: PacketId) extends Request[A]
  final case class UnregisterConnection[A](connectionId: ByteString) extends Request[A]
  final case class Route[A](clientId: Option[String], packetId: PacketId, event: A, failureReply: Promise[_])
      extends Request[A]
  final case class RouteViaConnection[A](connectionId: ByteString,
                                         packetId: PacketId,
                                         event: A,
                                         failureReply: Promise[_])
      extends Request[A]

  // Replies

  sealed abstract class Reply
  final case object Registered extends Reply

  /*
   * Construct with the starting state
   */
  def apply[A]: Behavior[Request[A]] =
    new RemotePacketRouter[A].main(Map.empty, Map.empty)

  private[streaming] case class Registration[A](registrant: ActorRef[A], failureReplies: Seq[Promise[_]])
}

/*
 * Route remotely generated MQTT packets based on packet identifiers.
 * Callers are able to request that they be registered for routing
 * along with a packet id received from the remote. These
 * callers are then watched for termination so that housekeeping can
 * be performed. The contract is therefore for a caller to initially register,
 * and to terminate when it has finished with the packet identifier.
 */
@InternalApi private[streaming] final class RemotePacketRouter[A] {

  import RemotePacketRouter._

  // Processing

  def main(registrantsByPacketId: Map[(Option[String], PacketId), Registration[A]],
           clientIdsByConnectionId: Map[ByteString, String]): Behavior[Request[A]] =
    Behaviors.receive {
      case (context, Register(registrant: ActorRef[A], clientId, packetId, reply)) =>
        reply.success(Registered)
        context.watchWith(registrant, Unregister(clientId, packetId))
        val key = (clientId, packetId)
        main(registrantsByPacketId + (key -> Registration(registrant, List.empty)), clientIdsByConnectionId)
      case (_, RegisterConnection(connectionId, clientId)) =>
        main(registrantsByPacketId, clientIdsByConnectionId + (connectionId -> clientId))
      case (_, Unregister(clientId, packetId)) =>
        // We tidy up and fail any failure promises that haven't already been failed -
        // just in case the registrant terminated abnormally and didn't get to complete
        // the promise. We all know that uncompleted promises can lead to memory leaks.
        // The known condition by which we'd succeed in failing the promise here is
        // when we thought we were able to route to a registrant, but the routing
        // subsequently failed, ending up the in the deadletter queue.
        registrantsByPacketId.get((clientId, packetId)).toList.flatMap(_.failureReplies).foreach { failureReply =>
          failureReply.tryFailure(CannotRoute(packetId))
        }
        val key = (clientId, packetId)
        main(registrantsByPacketId - key, clientIdsByConnectionId)
      case (_, UnregisterConnection(connectionId)) =>
        main(registrantsByPacketId, clientIdsByConnectionId - connectionId)
      case (_, Route(clientId, packetId, event, failureReply)) =>
        val key = (clientId, packetId)
        registrantsByPacketId.get(key) match {
          case Some(registration) =>
            registration.registrant ! event
            main(
              registrantsByPacketId.updated(
                (clientId, packetId),
                registration.copy(failureReplies = failureReply +: registration.failureReplies)
              ),
              clientIdsByConnectionId
            )
          case None =>
            failureReply.failure(CannotRoute(packetId))
            Behaviors.same
        }
      case (_, RouteViaConnection(connectionId, packetId, event, failureReply)) =>
        clientIdsByConnectionId.get(connectionId) match {
          case clientId: Some[String] =>
            val key = (clientId, packetId)
            registrantsByPacketId.get(key) match {
              case Some(registration) =>
                registration.registrant ! event
                main(
                  registrantsByPacketId.updated(
                    (clientId, packetId),
                    registration.copy(failureReplies = failureReply +: registration.failureReplies)
                  ),
                  clientIdsByConnectionId
                )
              case None =>
                failureReply.failure(CannotRoute(packetId))
                Behaviors.same
            }
          case None =>
            failureReply.failure(CannotRoute(packetId))
            Behaviors.same
        }
    }
}

object Topics {

  /*
   * 4.7 Topic Names and Topic Filters
   * http://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html
   *
   * Inspired by https://github.com/eclipse/paho.mqtt.java/blob/master/org.eclipse.paho.client.mqttv3/src/main/java/org/eclipse/paho/client/mqttv3/MqttTopic.java#L240
   */
  def filter(topicFilterName: String, topicName: String): Boolean = {
    @tailrec
    def matchStrings(tfn: String, tn: String): Boolean =
      if (tfn == "/+" && tn == "/") {
        true
      } else if (tfn.nonEmpty && tn.nonEmpty) {
        val tfnHead = tfn.charAt(0)
        val tnHead = tn.charAt(0)
        if (tfnHead == '/' && tnHead != '/') {
          false
        } else if (tfnHead == '/' && tnHead == '/' && tn.length == 1) {
          matchStrings(tfn, tn.tail)
        } else if (tfnHead != '+' && tfnHead != '#' && tfnHead != tnHead) {
          false
        } else if (tfnHead == '+') {
          matchStrings(tfn.tail, tn.tail.dropWhile(_ != '/'))
        } else if (tfnHead == '#') {
          matchStrings(tfn.tail, "")
        } else {
          matchStrings(tfn.tail, tn.tail)
        }
      } else if (tfn.isEmpty && tn.isEmpty) {
        true
      } else if (tfn == "/#" && tn.isEmpty) {
        true
      } else {
        false
      }
    matchStrings(topicFilterName, topicName)
  }
}
