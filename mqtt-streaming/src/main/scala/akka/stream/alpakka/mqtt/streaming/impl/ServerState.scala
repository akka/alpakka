/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.mqtt.streaming
package impl

import java.util.concurrent.TimeUnit

import akka.NotUsed
import akka.actor.typed.scaladsl.{ActorContext, Behaviors}
import akka.actor.typed.{ActorRef, Behavior, ChildFailed, PostStop, Terminated}
import akka.annotation.InternalApi
import akka.stream.{Materializer, OverflowStrategy}
import akka.stream.scaladsl.{BroadcastHub, Keep, Sink, Source, SourceQueueWithComplete}
import akka.util.ByteString

import scala.annotation.tailrec
import scala.concurrent.Promise
import scala.concurrent.duration.FiniteDuration
import scala.util.control.NoStackTrace
import scala.util.{Failure, Success}

/*
 * A server connector is a Finite State Machine that manages MQTT client
 * connections. A server receives connections from a client and
 * manages their subscriptions along with receiving publications
 * publishing them to subscribed topics.
 */
@InternalApi private[streaming] object ServerConnector {

  /*
   * A PINGREQ was not received within the required keep alive period - the connection must close
   *
   * http://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html
   * 3.1.2.10 Keep Alive
   */
  case object PingFailed extends Exception with NoStackTrace

  /*
   * Used to signal that a client connection has terminated
   */
  final case class ClientSessionTerminated(clientId: String)

  /*
   * Construct with the starting state
   */
  def apply(terminations: SourceQueueWithComplete[ClientSessionTerminated],
            consumerPacketRouter: ActorRef[RemotePacketRouter.Request[Consumer.Event]],
            producerPacketRouter: ActorRef[LocalPacketRouter.Request[Producer.Event]],
            publisherPacketRouter: ActorRef[RemotePacketRouter.Request[Publisher.Event]],
            unpublisherPacketRouter: ActorRef[RemotePacketRouter.Request[Unpublisher.Event]],
            settings: MqttSessionSettings)(implicit mat: Materializer): Behavior[Event] =
    listening(
      Data(Map.empty,
           terminations,
           consumerPacketRouter,
           producerPacketRouter,
           publisherPacketRouter,
           unpublisherPacketRouter,
           settings)
    )

  // Our FSM data, FSM events and commands emitted by the FSM

  final case class Data(clientConnections: Map[ByteString, (String, ActorRef[ClientConnection.Event])],
                        terminations: SourceQueueWithComplete[ClientSessionTerminated],
                        consumerPacketRouter: ActorRef[RemotePacketRouter.Request[Consumer.Event]],
                        producerPacketRouter: ActorRef[LocalPacketRouter.Request[Producer.Event]],
                        publisherPacketRouter: ActorRef[RemotePacketRouter.Request[Publisher.Event]],
                        unpublisherPacketRouter: ActorRef[RemotePacketRouter.Request[Unpublisher.Event]],
                        settings: MqttSessionSettings)

  sealed abstract class Event(val connectionId: ByteString)
  final case class ConnectReceivedFromRemote(override val connectionId: ByteString,
                                             connect: Connect,
                                             local: Promise[ClientConnection.ForwardConnect.type])
      extends Event(connectionId)
  final case class ReceiveConnAckTimeout(override val connectionId: ByteString) extends Event(connectionId)
  final case class ConnAckReceivedLocally(override val connectionId: ByteString,
                                          connAck: ConnAck,
                                          remote: Promise[Source[ClientConnection.ForwardConnAckCommand, NotUsed]])
      extends Event(connectionId)
  final case class SubscribeReceivedFromRemote(override val connectionId: ByteString,
                                               subscribe: Subscribe,
                                               local: Promise[Publisher.ForwardSubscribe.type])
      extends Event(connectionId)
  final case class PublishReceivedFromRemote(override val connectionId: ByteString,
                                             publish: Publish,
                                             local: Promise[Consumer.ForwardPublish.type])
      extends Event(connectionId)
  final case class PublishReceivedLocally(publish: Publish, publishData: Producer.PublishData)
      extends Event(ByteString.empty)
  final case class UnsubscribeReceivedFromRemote(override val connectionId: ByteString,
                                                 unsubscribe: Unsubscribe,
                                                 local: Promise[Unpublisher.ForwardUnsubscribe.type])
      extends Event(connectionId)
  final case class PingReqReceivedFromRemote(override val connectionId: ByteString,
                                             local: Promise[ClientConnection.ForwardPingReq.type])
      extends Event(connectionId)
  final case class DisconnectReceivedFromRemote(override val connectionId: ByteString,
                                                local: Promise[ClientConnection.ForwardDisconnect.type])
      extends Event(connectionId)
  final case class ConnectionLost(override val connectionId: ByteString) extends Event(connectionId)

  // State event handling

  private val ClientConnectionNamePrefix = "client-connection-"

  private def forward(connectionId: ByteString,
                      clientConnections: Map[ByteString, (String, ActorRef[ClientConnection.Event])],
                      e: ClientConnection.Event): Behavior[Event] = {
    clientConnections.get(connectionId).foreach { case (_, cc) => cc ! e }
    Behaviors.same
  }

  def listening(data: Data)(implicit mat: Materializer): Behavior[Event] = {
    def childTerminated(terminatedCc: ActorRef[ClientConnection.Event]): Behavior[Event] =
      data.clientConnections.find { case (_, (_, cc)) => cc == terminatedCc } match {
        case Some((connectionId, (clientId, _))) =>
          data.terminations.offer(ClientSessionTerminated(clientId))
          data.consumerPacketRouter ! RemotePacketRouter.UnregisterConnection(connectionId)
          data.publisherPacketRouter ! RemotePacketRouter.UnregisterConnection(connectionId)
          data.unpublisherPacketRouter ! RemotePacketRouter.UnregisterConnection(connectionId)
          listening(data.copy(clientConnections = data.clientConnections - connectionId))
        case None =>
          Behaviors.same
      }

    Behaviors
      .receivePartial[Event] {
        case (context, ConnectReceivedFromRemote(connectionId, connect, local)) =>
          val clientConnectionName = ActorName.mkName(ClientConnectionNamePrefix + connect.clientId)
          val clientConnection = context.child(clientConnectionName) match {
            case None =>
              context.spawn(
                ClientConnection(connect,
                                 local,
                                 data.consumerPacketRouter,
                                 data.producerPacketRouter,
                                 data.publisherPacketRouter,
                                 data.unpublisherPacketRouter,
                                 data.settings),
                clientConnectionName
              )

            case Some(ref) =>
              val cc = ref.unsafeUpcast[ClientConnection.Event]
              cc ! ClientConnection.ConnectReceivedFromRemote(connect, local)
              cc
          }
          context.watch(clientConnection)
          data.consumerPacketRouter ! RemotePacketRouter.RegisterConnection(connectionId, connect.clientId)
          data.publisherPacketRouter ! RemotePacketRouter.RegisterConnection(connectionId, connect.clientId)
          data.unpublisherPacketRouter ! RemotePacketRouter.RegisterConnection(connectionId, connect.clientId)
          val newConnection = (connectionId, (connect.clientId, clientConnection))
          listening(
            data.copy(
              clientConnections = data.clientConnections
                .filterNot { case (_, (clientId, _)) => clientId == connect.clientId } + newConnection
            )
          )
        case (_, ConnAckReceivedLocally(connectionId, connAck, remote)) =>
          forward(connectionId, data.clientConnections, ClientConnection.ConnAckReceivedLocally(connAck, remote))
        case (_, SubscribeReceivedFromRemote(connectionId, subscribe, local)) =>
          forward(connectionId, data.clientConnections, ClientConnection.SubscribeReceivedFromRemote(subscribe, local))
        case (_, PublishReceivedFromRemote(connectionId, publish, local)) =>
          forward(connectionId, data.clientConnections, ClientConnection.PublishReceivedFromRemote(publish, local))
        case (_, PublishReceivedLocally(publish, publishData)) =>
          data.clientConnections.values.foreach {
            case (_, cc) => cc ! ClientConnection.PublishReceivedLocally(publish, publishData)
          }
          Behaviors.same
        case (_, UnsubscribeReceivedFromRemote(connectionId, unsubscribe, local)) =>
          forward(connectionId,
                  data.clientConnections,
                  ClientConnection.UnsubscribeReceivedFromRemote(unsubscribe, local))
        case (_, PingReqReceivedFromRemote(connectionId, local)) =>
          forward(connectionId, data.clientConnections, ClientConnection.PingReqReceivedFromRemote(local))
        case (_, DisconnectReceivedFromRemote(connectionId, local)) =>
          forward(connectionId, data.clientConnections, ClientConnection.DisconnectReceivedFromRemote(local))
        case (_, ConnectionLost(connectionId)) =>
          forward(connectionId, data.clientConnections, ClientConnection.ConnectionLost)
      }
      .receiveSignal {
        case (_, Terminated(ref)) =>
          childTerminated(ref.unsafeUpcast[ClientConnection.Event])
        case (_, ChildFailed(ref, failure)) if failure == ClientConnection.ClientConnectionFailed =>
          childTerminated(ref.unsafeUpcast[ClientConnection.Event])
      }
  }
}

/*
 * Handles events in relation to a specific client connection
 */
@InternalApi private[streaming] object ClientConnection {

  /*
   * No ACK received - the subscription failed
   */
  case object ClientConnectionFailed extends Exception with NoStackTrace

  /*
   * A PINGREQ was not received within 1.5 times the keep alive so the connection will close
   *
   * http://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html
   * 3.1.2.10 Keep Alive
   */
  case object PingFailed extends Exception with NoStackTrace

  /*
   * Construct with the starting state
   */
  def apply(connect: Connect,
            local: Promise[ForwardConnect.type],
            consumerPacketRouter: ActorRef[RemotePacketRouter.Request[Consumer.Event]],
            producerPacketRouter: ActorRef[LocalPacketRouter.Request[Producer.Event]],
            publisherPacketRouter: ActorRef[RemotePacketRouter.Request[Publisher.Event]],
            unpublisherPacketRouter: ActorRef[RemotePacketRouter.Request[Unpublisher.Event]],
            settings: MqttSessionSettings)(implicit mat: Materializer): Behavior[Event] =
    clientConnect(
      ConnectReceived(
        connect,
        local,
        Set.empty,
        Set.empty,
        Set.empty,
        Vector.empty,
        Vector.empty,
        Vector.empty,
        consumerPacketRouter,
        producerPacketRouter,
        publisherPacketRouter,
        unpublisherPacketRouter,
        settings
      )
    )

  // Our FSM data, FSM events and commands emitted by the FSM

  sealed abstract class Data(val consumerPacketRouter: ActorRef[RemotePacketRouter.Request[Consumer.Event]],
                             val producerPacketRouter: ActorRef[LocalPacketRouter.Request[Producer.Event]],
                             val publisherPacketRouter: ActorRef[RemotePacketRouter.Request[Publisher.Event]],
                             val unpublisherPacketRouter: ActorRef[RemotePacketRouter.Request[Unpublisher.Event]],
                             val settings: MqttSessionSettings)
  final case class ConnectReceived(
      connect: Connect,
      local: Promise[ForwardConnect.type],
      publishers: Set[String],
      activeConsumers: Set[String],
      activeProducers: Set[String],
      pendingLocalPublications: Seq[(String, PublishReceivedLocally)],
      pendingRemotePublications: Seq[(String, PublishReceivedFromRemote)],
      stash: Seq[Event],
      override val consumerPacketRouter: ActorRef[RemotePacketRouter.Request[Consumer.Event]],
      override val producerPacketRouter: ActorRef[LocalPacketRouter.Request[Producer.Event]],
      override val publisherPacketRouter: ActorRef[RemotePacketRouter.Request[Publisher.Event]],
      override val unpublisherPacketRouter: ActorRef[RemotePacketRouter.Request[Unpublisher.Event]],
      override val settings: MqttSessionSettings
  ) extends Data(consumerPacketRouter, producerPacketRouter, publisherPacketRouter, unpublisherPacketRouter, settings)
  final case class ConnAckReplied(
      connect: Connect,
      remote: SourceQueueWithComplete[ForwardConnAckCommand],
      publishers: Set[String],
      activeConsumers: Set[String],
      activeProducers: Set[String],
      pendingLocalPublications: Seq[(String, PublishReceivedLocally)],
      pendingRemotePublications: Seq[(String, PublishReceivedFromRemote)],
      override val consumerPacketRouter: ActorRef[RemotePacketRouter.Request[Consumer.Event]],
      override val producerPacketRouter: ActorRef[LocalPacketRouter.Request[Producer.Event]],
      override val publisherPacketRouter: ActorRef[RemotePacketRouter.Request[Publisher.Event]],
      override val unpublisherPacketRouter: ActorRef[RemotePacketRouter.Request[Unpublisher.Event]],
      override val settings: MqttSessionSettings
  ) extends Data(consumerPacketRouter, producerPacketRouter, publisherPacketRouter, unpublisherPacketRouter, settings)
  final case class PendingSubscribe(
      subscribe: Subscribe,
      connect: Connect,
      remote: SourceQueueWithComplete[ForwardConnAckCommand],
      publishers: Set[String],
      activeConsumers: Set[String],
      activeProducers: Set[String],
      pendingLocalPublications: Seq[(String, PublishReceivedLocally)],
      pendingRemotePublications: Seq[(String, PublishReceivedFromRemote)],
      stash: Seq[Event],
      override val consumerPacketRouter: ActorRef[RemotePacketRouter.Request[Consumer.Event]],
      override val producerPacketRouter: ActorRef[LocalPacketRouter.Request[Producer.Event]],
      override val publisherPacketRouter: ActorRef[RemotePacketRouter.Request[Publisher.Event]],
      override val unpublisherPacketRouter: ActorRef[RemotePacketRouter.Request[Unpublisher.Event]],
      override val settings: MqttSessionSettings
  ) extends Data(consumerPacketRouter, producerPacketRouter, publisherPacketRouter, unpublisherPacketRouter, settings)
  final case class Disconnected(
      publishers: Set[String],
      activeConsumers: Set[String],
      activeProducers: Set[String],
      pendingLocalPublications: Seq[(String, PublishReceivedLocally)],
      pendingRemotePublications: Seq[(String, PublishReceivedFromRemote)],
      override val consumerPacketRouter: ActorRef[RemotePacketRouter.Request[Consumer.Event]],
      override val producerPacketRouter: ActorRef[LocalPacketRouter.Request[Producer.Event]],
      override val publisherPacketRouter: ActorRef[RemotePacketRouter.Request[Publisher.Event]],
      override val unpublisherPacketRouter: ActorRef[RemotePacketRouter.Request[Unpublisher.Event]],
      override val settings: MqttSessionSettings
  ) extends Data(consumerPacketRouter, producerPacketRouter, publisherPacketRouter, unpublisherPacketRouter, settings)

  sealed abstract class Event
  case object ReceiveConnAckTimeout extends Event
  final case class ConnAckReceivedLocally(connAck: ConnAck, remote: Promise[Source[ForwardConnAckCommand, NotUsed]])
      extends Event
  final case class SubscribeReceivedFromRemote(subscribe: Subscribe, local: Promise[Publisher.ForwardSubscribe.type])
      extends Event
  final case class PublishReceivedFromRemote(publish: Publish, local: Promise[Consumer.ForwardPublish.type])
      extends Event
  final case class ConsumerFree(topicName: String) extends Event
  final case class PublishReceivedLocally(publish: Publish, publishData: Producer.PublishData) extends Event
  final case class ProducerFree(topicName: String) extends Event
  final case class UnsubscribeReceivedFromRemote(unsubscribe: Unsubscribe,
                                                 local: Promise[Unpublisher.ForwardUnsubscribe.type])
      extends Event
  final case class PingReqReceivedFromRemote(local: Promise[ForwardPingReq.type]) extends Event
  final case class DisconnectReceivedFromRemote(local: Promise[ForwardDisconnect.type]) extends Event
  case object ConnectionLost extends Event
  final case class UnpublisherFree(topicFilters: Seq[String]) extends Event
  case object ReceivePingReqTimeout extends Event
  final case class ReceivedProducerPublishingCommand(command: Source[Producer.ForwardPublishingCommand, NotUsed])
      extends Event
  final case class ConnectReceivedFromRemote(connect: Connect, local: Promise[ClientConnection.ForwardConnect.type])
      extends Event
  case object ReceiveConnectTimeout extends Event

  sealed abstract class Command
  case object ForwardConnect extends Command
  sealed abstract class ForwardConnAckCommand
  case object ForwardConnAck extends ForwardConnAckCommand
  case object ForwardPingReq extends Command
  case object ForwardPingResp extends ForwardConnAckCommand
  case object ForwardDisconnect extends Command
  final case class ForwardPublish(publish: Publish, packetId: Option[PacketId]) extends ForwardConnAckCommand
  final case class ForwardPubRel(packetId: PacketId) extends ForwardConnAckCommand

  // State event handling

  private val ConsumerNamePrefix = "consumer-"
  private val ProducerNamePrefix = "producer-"
  private val PublisherNamePrefix = "publisher-"
  private val UnpublisherNamePrefix = "unpublisher-"

  def clientConnect(data: ConnectReceived)(implicit mat: Materializer): Behavior[Event] = Behaviors.setup { _ =>
    data.local.trySuccess(ForwardConnect)

    Behaviors.withTimers { timer =>
      timer.startSingleTimer("receive-connack", ReceiveConnAckTimeout, data.settings.receiveConnAckTimeout)

      Behaviors
        .receivePartial[Event] {
          case (context, ConnAckReceivedLocally(_, remote)) =>
            val (queue, source) = Source
              .queue[ForwardConnAckCommand](data.settings.serverSendBufferSize, OverflowStrategy.dropNew)
              .toMat(BroadcastHub.sink)(Keep.both)
              .run()
            remote.success(source)

            queue.offer(ForwardConnAck)
            data.stash.foreach(context.self.tell)

            clientConnected(
              ConnAckReplied(
                data.connect,
                queue,
                data.publishers,
                data.activeConsumers,
                data.activeProducers,
                data.pendingLocalPublications,
                data.pendingRemotePublications,
                data.consumerPacketRouter,
                data.producerPacketRouter,
                data.publisherPacketRouter,
                data.unpublisherPacketRouter,
                data.settings
              )
            )
          case (_, ReceiveConnAckTimeout) =>
            throw ClientConnectionFailed
          case (_, ClientConnection.ConnectionLost) =>
            throw ClientConnectionFailed
          case (_, e) =>
            clientConnect(data.copy(stash = data.stash :+ e))
        }
        .receiveSignal {
          case (_, _: Terminated) =>
            Behaviors.same
        }
    }
  }

  def clientConnected(data: ConnAckReplied)(implicit mat: Materializer): Behavior[Event] = Behaviors.withTimers {
    timer =>
      if (data.connect.keepAlive.toMillis > 0)
        timer.startSingleTimer("receive-pingreq",
                               ReceivePingReqTimeout,
                               FiniteDuration((data.connect.keepAlive.toMillis * 1.5).toLong, TimeUnit.MILLISECONDS))

      Behaviors
        .receivePartial[Event] {
          case (context, SubscribeReceivedFromRemote(subscribe, local)) =>
            val publisherName = ActorName.mkName(PublisherNamePrefix + subscribe.topicFilters)
            context.child(publisherName) match {
              case None =>
                val publisher = context.spawn(
                  Publisher(data.connect.clientId,
                            subscribe.packetId,
                            local,
                            data.publisherPacketRouter,
                            data.settings),
                  publisherName
                )
                context.watch(publisher)
                pendingSubAck(
                  PendingSubscribe(
                    subscribe,
                    data.connect,
                    data.remote,
                    data.publishers,
                    data.activeConsumers,
                    data.activeProducers,
                    data.pendingLocalPublications,
                    data.pendingRemotePublications,
                    Vector.empty,
                    data.consumerPacketRouter,
                    data.producerPacketRouter,
                    data.publisherPacketRouter,
                    data.unpublisherPacketRouter,
                    data.settings
                  )
                )
              case _: Some[_] => // It is an error to get here
                local
                  .failure(new IllegalStateException("Shouldn't be able to receive subscriptions here: " + subscribe))
                Behaviors.same
            }
          case (context, UnsubscribeReceivedFromRemote(unsubscribe, local)) =>
            val unpublisherName = ActorName.mkName(UnpublisherNamePrefix + unsubscribe.topicFilters)
            context.child(unpublisherName) match {
              case None =>
                val unpublisher = context.spawn(
                  Unpublisher(data.connect.clientId,
                              unsubscribe.packetId,
                              local,
                              data.unpublisherPacketRouter,
                              data.settings),
                  unpublisherName
                )
                context.watchWith(unpublisher, UnpublisherFree(unsubscribe.topicFilters))
              case _: Some[_] =>
                local.failure(new IllegalStateException("Duplicate unsubscribe: " + unsubscribe))
            }
            Behaviors.same
          case (_, UnpublisherFree(topicFilters)) =>
            val unsubscribedTopicFilters =
              data.publishers.filter(publisher => topicFilters.exists(matchTopicFilter(_, publisher)))
            clientConnected(data.copy(publishers = data.publishers -- unsubscribedTopicFilters))
          case (_, PublishReceivedFromRemote(publish, local))
              if (publish.flags & ControlPacketFlags.QoSReserved).underlying == 0 =>
            local.success(Consumer.ForwardPublish)
            clientConnected(data)
          case (context, prfr @ PublishReceivedFromRemote(publish @ Publish(_, topicName, Some(packetId), _), local)) =>
            if (!data.activeConsumers.contains(topicName)) {
              val consumerName = ActorName.mkName(ConsumerNamePrefix + topicName + "-" + context.children.size)
              context.watchWith(
                context.spawn(
                  Consumer(publish,
                           Some(data.connect.clientId),
                           packetId,
                           local,
                           data.consumerPacketRouter,
                           data.settings),
                  consumerName
                ),
                ConsumerFree(publish.topicName)
              )
              clientConnected(data.copy(activeConsumers = data.activeConsumers + publish.topicName))
            } else {
              clientConnected(
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
                           Some(data.connect.clientId),
                           prfr.publish.packetId.get,
                           prfr.local,
                           data.consumerPacketRouter,
                           data.settings),
                  consumerName
                ),
                ConsumerFree(topicName)
              )
              clientConnected(
                data.copy(
                  pendingRemotePublications =
                  data.pendingRemotePublications.take(i) ++ data.pendingRemotePublications.drop(i + 1)
                )
              )
            } else {
              clientConnected(data.copy(activeConsumers = data.activeConsumers - topicName))
            }
          case (_, PublishReceivedLocally(publish, _))
              if (publish.flags & ControlPacketFlags.QoSReserved).underlying == 0 &&
              data.publishers.exists(matchTopicFilter(_, publish.topicName)) =>
            data.remote.offer(ForwardPublish(publish, None))
            clientConnected(data)
          case (context, prl @ PublishReceivedLocally(publish, publishData))
              if data.publishers.exists(matchTopicFilter(_, publish.topicName)) =>
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
              clientConnected(data.copy(activeProducers = data.activeProducers + publish.topicName))
            } else {
              clientConnected(
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
              clientConnected(
                data.copy(
                  pendingLocalPublications =
                  data.pendingLocalPublications.take(i) ++ data.pendingLocalPublications.drop(i + 1)
                )
              )
            } else {
              clientConnected(data.copy(activeProducers = data.activeProducers - topicName))
            }
          case (_, ReceivedProducerPublishingCommand(command)) =>
            command.runWith(Sink.foreach {
              case Producer.ForwardPublish(publish, packetId) => data.remote.offer(ForwardPublish(publish, packetId))
              case Producer.ForwardPubRel(_, packetId) => data.remote.offer(ForwardPubRel(packetId))
            })
            Behaviors.same
          case (_, PingReqReceivedFromRemote(local)) =>
            data.remote.offer(ForwardPingResp)
            local.success(ForwardPingReq)
            clientConnected(data)
          case (_, ReceivePingReqTimeout) =>
            data.remote.fail(PingFailed)
            clientDisconnected(
              Disconnected(
                data.publishers,
                data.activeConsumers,
                data.activeProducers,
                data.pendingLocalPublications,
                data.pendingRemotePublications,
                data.consumerPacketRouter,
                data.producerPacketRouter,
                data.publisherPacketRouter,
                data.unpublisherPacketRouter,
                data.settings
              )
            )
          case (_, DisconnectReceivedFromRemote(local)) =>
            local.success(ForwardDisconnect)
            clientDisconnected(
              Disconnected(
                data.publishers,
                data.activeConsumers,
                data.activeProducers,
                data.pendingLocalPublications,
                data.pendingRemotePublications,
                data.consumerPacketRouter,
                data.producerPacketRouter,
                data.publisherPacketRouter,
                data.unpublisherPacketRouter,
                data.settings
              )
            )
          case (_, ClientConnection.ConnectionLost) =>
            clientDisconnected(
              Disconnected(
                data.publishers,
                data.activeConsumers,
                data.activeProducers,
                data.pendingLocalPublications,
                data.pendingRemotePublications,
                data.consumerPacketRouter,
                data.producerPacketRouter,
                data.publisherPacketRouter,
                data.unpublisherPacketRouter,
                data.settings
              )
            )
          case (context, ConnectReceivedFromRemote(connect, local))
              if connect.connectFlags.contains(ConnectFlags.CleanSession) =>
            context.children.foreach(context.stop)
            clientConnect(
              ConnectReceived(
                connect,
                local,
                Set.empty,
                Set.empty,
                Set.empty,
                Vector.empty,
                Vector.empty,
                Vector.empty,
                data.consumerPacketRouter,
                data.producerPacketRouter,
                data.publisherPacketRouter,
                data.unpublisherPacketRouter,
                data.settings
              )
            )
          case (_, ConnectReceivedFromRemote(connect, local)) =>
            clientConnect(
              ConnectReceived(
                connect,
                local,
                data.publishers,
                data.activeConsumers,
                data.activeProducers,
                data.pendingLocalPublications,
                data.pendingRemotePublications,
                Vector.empty,
                data.consumerPacketRouter,
                data.producerPacketRouter,
                data.publisherPacketRouter,
                data.unpublisherPacketRouter,
                data.settings
              )
            )
        }
        .receiveSignal {
          case (_, _: Terminated) =>
            Behaviors.same
          case (_, PostStop) =>
            data.remote.complete()
            Behaviors.same
        }
  }

  def pendingSubAck(data: PendingSubscribe)(implicit mat: Materializer): Behavior[Event] = {
    def childTerminated(context: ActorContext[ClientConnection.Event], failure: Option[Throwable]): Behavior[Event] = {
      data.stash.foreach(context.self.tell)
      clientConnected(
        ConnAckReplied(
          data.connect,
          data.remote,
          data.publishers ++ failure.fold(data.subscribe.topicFilters.map(_._1))(_ => Vector.empty),
          data.activeConsumers,
          data.activeProducers,
          data.pendingLocalPublications,
          data.pendingRemotePublications,
          data.consumerPacketRouter,
          data.producerPacketRouter,
          data.publisherPacketRouter,
          data.unpublisherPacketRouter,
          data.settings
        )
      )
    }
    Behaviors
      .receivePartial[Event] {
        case (_, e) =>
          pendingSubAck(data.copy(stash = data.stash :+ e))
      }
      .receiveSignal {
        case (context, ChildFailed(_, failure)) if failure == Publisher.SubscribeFailed =>
          childTerminated(context, Some(failure))
        case (context, _: Terminated) =>
          childTerminated(context, None)
        case (_, PostStop) =>
          data.remote.complete()
          Behaviors.same
      }
  }

  def clientDisconnected(data: Disconnected)(implicit mat: Materializer): Behavior[Event] = Behaviors.withTimers {
    timer =>
      timer.startSingleTimer("receive-connect", ReceiveConnectTimeout, data.settings.receiveConnectTimeout)

      Behaviors
        .receivePartial[Event] {
          case (context, ConnectReceivedFromRemote(connect, local))
              if connect.connectFlags.contains(ConnectFlags.CleanSession) =>
            context.children.foreach(context.stop)
            clientConnect(
              ConnectReceived(
                connect,
                local,
                Set.empty,
                Set.empty,
                Set.empty,
                Vector.empty,
                Vector.empty,
                Vector.empty,
                data.consumerPacketRouter,
                data.producerPacketRouter,
                data.publisherPacketRouter,
                data.unpublisherPacketRouter,
                data.settings
              )
            )
          case (_, ConnectReceivedFromRemote(connect, local)) =>
            clientConnect(
              ConnectReceived(
                connect,
                local,
                data.publishers,
                data.activeConsumers,
                data.activeProducers,
                data.pendingLocalPublications,
                data.pendingRemotePublications,
                Vector.empty,
                data.consumerPacketRouter,
                data.producerPacketRouter,
                data.publisherPacketRouter,
                data.unpublisherPacketRouter,
                data.settings
              )
            )
          case (_, ReceiveConnectTimeout) =>
            throw ClientConnectionFailed
        }
        .receiveSignal {
          case (_, _: Terminated) =>
            Behaviors.same
        }
  }

  //

  /*
   * 4.7 Topic Names and Topic Filters
   * http://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html
   *
   * Inspired by https://github.com/eclipse/paho.mqtt.java/blob/master/org.eclipse.paho.client.mqttv3/src/main/java/org/eclipse/paho/client/mqttv3/MqttTopic.java#L240
   */
  def matchTopicFilter(topicFilterName: String, topicName: String): Boolean = {
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

/*
 * A publisher manages the client state in relation to having received a
 * subscription to a server-side topic. A publisher is created
 * per client per topic filter.
 */
@InternalApi private[streaming] object Publisher {

  /*
   * No ACK received - the subscription failed
   */
  case object SubscribeFailed extends Exception with NoStackTrace

  /*
   * Construct with the starting state
   */
  def apply(clientId: String,
            packetId: PacketId,
            local: Promise[ForwardSubscribe.type],
            packetRouter: ActorRef[RemotePacketRouter.Request[Event]],
            settings: MqttSessionSettings): Behavior[Event] =
    preparePublisher(Start(Some(clientId), packetId, local, packetRouter, settings))

  // Our FSM data, FSM events and commands emitted by the FSM

  sealed abstract class Data(val clientId: Some[String], val packetId: PacketId, val settings: MqttSessionSettings)
  final case class Start(override val clientId: Some[String],
                         override val packetId: PacketId,
                         local: Promise[ForwardSubscribe.type],
                         packetRouter: ActorRef[RemotePacketRouter.Request[Event]],
                         override val settings: MqttSessionSettings)
      extends Data(clientId, packetId, settings)
  final case class ServerSubscribe(override val clientId: Some[String],
                                   override val packetId: PacketId,
                                   packetRouter: ActorRef[RemotePacketRouter.Request[Event]],
                                   override val settings: MqttSessionSettings)
      extends Data(clientId, packetId, settings)

  sealed abstract class Event
  final case object RegisteredPacketId extends Event
  final case object UnobtainablePacketId extends Event
  final case class SubAckReceivedLocally(remote: Promise[ForwardSubAck.type]) extends Event
  case object ReceiveSubAckTimeout extends Event

  sealed abstract class Command
  case object ForwardSubscribe extends Command
  case object ForwardSubAck extends Command

  // State event handling

  def preparePublisher(data: Start): Behavior[Event] = Behaviors.setup { context =>
    val reply = Promise[RemotePacketRouter.Registered.type]
    data.packetRouter ! RemotePacketRouter.Register(context.self.unsafeUpcast, data.clientId, data.packetId, reply)
    import context.executionContext
    reply.future.onComplete {
      case Success(RemotePacketRouter.Registered) => context.self ! RegisteredPacketId
      case Failure(_) => context.self ! UnobtainablePacketId
    }

    Behaviors.receiveMessagePartial[Event] {
      case RegisteredPacketId =>
        data.local.success(ForwardSubscribe)
        serverSubscribe(ServerSubscribe(data.clientId, data.packetId, data.packetRouter, data.settings))
      case UnobtainablePacketId =>
        data.local.failure(SubscribeFailed)
        throw SubscribeFailed
    }

  }

  def serverSubscribe(data: ServerSubscribe): Behavior[Event] = Behaviors.withTimers { timer =>
    timer.startSingleTimer("receive-suback", ReceiveSubAckTimeout, data.settings.receiveSubAckTimeout)

    Behaviors
      .receiveMessagePartial[Event] {
        case SubAckReceivedLocally(remote) =>
          remote.success(ForwardSubAck)
          Behaviors.stopped
        case ReceiveSubAckTimeout =>
          throw SubscribeFailed
      }
      .receiveSignal {
        case (_, PostStop) =>
          data.packetRouter ! RemotePacketRouter.Unregister(data.clientId, data.packetId)
          Behaviors.same
      }
  }
}

/*
 * A unpublisher manages the client state in relation to unsubscribing from a
 * server-side topic. A unpublisher is created per server per topic.
 */
@InternalApi private[streaming] object Unpublisher {

  /*
   * No ACK received - the unsubscription failed
   */
  case object UnsubscribeFailed extends Exception with NoStackTrace

  /*
   * Construct with the starting state
   */
  def apply(clientId: String,
            packetId: PacketId,
            local: Promise[ForwardUnsubscribe.type],
            packetRouter: ActorRef[RemotePacketRouter.Request[Event]],
            settings: MqttSessionSettings): Behavior[Event] =
    prepareServerUnpublisher(Start(Some(clientId), packetId, local, packetRouter, settings))

  // Our FSM data, FSM events and commands emitted by the FSM

  sealed abstract class Data(val settings: MqttSessionSettings)
  final case class Start(clientId: Some[String],
                         packetId: PacketId,
                         local: Promise[ForwardUnsubscribe.type],
                         packetRouter: ActorRef[RemotePacketRouter.Request[Event]],
                         override val settings: MqttSessionSettings)
      extends Data(settings)
  final case class ServerUnsubscribe(clientId: Some[String],
                                     packetId: PacketId,
                                     packetRouter: ActorRef[RemotePacketRouter.Request[Event]],
                                     override val settings: MqttSessionSettings)
      extends Data(settings)

  sealed abstract class Event
  final case object RegisteredPacketId extends Event
  final case object UnobtainablePacketId extends Event
  final case class UnsubAckReceivedLocally(remote: Promise[ForwardUnsubAck.type]) extends Event
  case object ReceiveUnsubAckTimeout extends Event

  sealed abstract class Command
  case object ForwardUnsubscribe extends Command
  case object ForwardUnsubAck extends Command

  // State event handling

  def prepareServerUnpublisher(data: Start): Behavior[Event] = Behaviors.setup { context =>
    val reply = Promise[RemotePacketRouter.Registered.type]
    data.packetRouter ! RemotePacketRouter.Register(context.self.unsafeUpcast, data.clientId, data.packetId, reply)
    import context.executionContext
    reply.future.onComplete {
      case Success(RemotePacketRouter.Registered) => context.self ! RegisteredPacketId
      case Failure(_) => context.self ! UnobtainablePacketId
    }

    Behaviors.receiveMessagePartial[Event] {
      case RegisteredPacketId =>
        data.local.success(ForwardUnsubscribe)
        serverUnsubscribe(ServerUnsubscribe(data.clientId, data.packetId, data.packetRouter, data.settings))
      case UnobtainablePacketId =>
        data.local.failure(UnsubscribeFailed)
        throw UnsubscribeFailed
    }
  }

  def serverUnsubscribe(data: ServerUnsubscribe): Behavior[Event] = Behaviors.withTimers { timer =>
    timer.startSingleTimer("receive-unsubAck", ReceiveUnsubAckTimeout, data.settings.receiveUnsubAckTimeout)

    Behaviors
      .receiveMessagePartial[Event] {
        case UnsubAckReceivedLocally(remote) =>
          remote.success(ForwardUnsubAck)
          Behaviors.stopped
        case ReceiveUnsubAckTimeout =>
          throw UnsubscribeFailed
      }
      .receiveSignal {
        case (_, PostStop) =>
          data.packetRouter ! RemotePacketRouter.Unregister(data.clientId, data.packetId)
          Behaviors.same
      }
  }
}
