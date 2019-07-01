/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.mqtt.streaming
package impl

import java.util.concurrent.TimeUnit

import akka.{Done, NotUsed}
import akka.actor.typed.scaladsl.{ActorContext, Behaviors}
import akka.actor.typed._
import akka.annotation.InternalApi
import akka.stream.{Materializer, OverflowStrategy, QueueOfferResult}
import akka.stream.scaladsl.{BroadcastHub, Keep, Source, SourceQueueWithComplete}
import akka.util.ByteString

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
  final case class QueueOfferCompleted(override val connectionId: ByteString,
                                       result: Either[Throwable, QueueOfferResult])
      extends Event(connectionId)
      with QueueOfferState.QueueOfferCompleted

  // State event handling

  private val ClientConnectionNamePrefix = "client-connection-"

  private def forward(connectionId: ByteString,
                      clientConnections: Map[ByteString, (String, ActorRef[ClientConnection.Event])],
                      e: ClientConnection.Event): Behavior[Event] = {
    clientConnections.get(connectionId).foreach { case (_, cc) => cc ! e }
    Behaviors.same
  }

  def listening(data: Data)(implicit mat: Materializer): Behavior[Event] = Behaviors.setup { context =>
    def childTerminated(terminatedCc: ActorRef[ClientConnection.Event]): Behavior[Event] =
      data.clientConnections.find { case (_, (_, cc)) => cc == terminatedCc } match {
        case Some((connectionId, (clientId, _))) =>
          import context.executionContext

          data.terminations
            .offer(ClientSessionTerminated(clientId))
            .onComplete(result => context.self.tell(QueueOfferCompleted(connectionId, result.toEither)))

          data.consumerPacketRouter ! RemotePacketRouter.UnregisterConnection(connectionId)
          data.publisherPacketRouter ! RemotePacketRouter.UnregisterConnection(connectionId)
          data.unpublisherPacketRouter ! RemotePacketRouter.UnregisterConnection(connectionId)

          QueueOfferState.waitForQueueOfferCompleted(
            listening(data.copy(clientConnections = data.clientConnections - connectionId)),
            stash = Seq.empty
          )

        case None =>
          Behaviors.same
      }

    Behaviors
      .receiveMessagePartial[Event] {
        case ConnectReceivedFromRemote(connectionId, connect, local) =>
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
        case ConnAckReceivedLocally(connectionId, connAck, remote) =>
          forward(connectionId, data.clientConnections, ClientConnection.ConnAckReceivedLocally(connAck, remote))
        case SubscribeReceivedFromRemote(connectionId, subscribe, local) =>
          forward(connectionId, data.clientConnections, ClientConnection.SubscribeReceivedFromRemote(subscribe, local))
        case PublishReceivedFromRemote(connectionId, publish, local) =>
          forward(connectionId, data.clientConnections, ClientConnection.PublishReceivedFromRemote(publish, local))
        case PublishReceivedLocally(publish, publishData) =>
          data.clientConnections.values.foreach {
            case (_, cc) => cc ! ClientConnection.PublishReceivedLocally(publish, publishData)
          }
          Behaviors.same
        case UnsubscribeReceivedFromRemote(connectionId, unsubscribe, local) =>
          forward(connectionId,
                  data.clientConnections,
                  ClientConnection.UnsubscribeReceivedFromRemote(unsubscribe, local))
        case PingReqReceivedFromRemote(connectionId, local) =>
          forward(connectionId, data.clientConnections, ClientConnection.PingReqReceivedFromRemote(local))
        case DisconnectReceivedFromRemote(connectionId, local) =>
          forward(connectionId, data.clientConnections, ClientConnection.DisconnectReceivedFromRemote(local))
        case ConnectionLost(connectionId) =>
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
   * No ACK received - the connection failed
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
        Vector.empty,
        Set.empty,
        Map.empty,
        Map.empty,
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

  sealed abstract class Data(val stash: Seq[Event],
                             val publishers: Set[String],
                             val activeConsumers: Map[String, ActorRef[Consumer.Event]],
                             val activeProducers: Map[String, ActorRef[Producer.Event]],
                             val pendingLocalPublications: Seq[(String, PublishReceivedLocally)],
                             val pendingRemotePublications: Seq[(String, PublishReceivedFromRemote)],
                             val consumerPacketRouter: ActorRef[RemotePacketRouter.Request[Consumer.Event]],
                             val producerPacketRouter: ActorRef[LocalPacketRouter.Request[Producer.Event]],
                             val publisherPacketRouter: ActorRef[RemotePacketRouter.Request[Publisher.Event]],
                             val unpublisherPacketRouter: ActorRef[RemotePacketRouter.Request[Unpublisher.Event]],
                             val settings: MqttSessionSettings)
  final case class ConnectReceived(
      connect: Connect,
      local: Promise[ForwardConnect.type],
      override val stash: Seq[Event],
      override val publishers: Set[String],
      override val activeConsumers: Map[String, ActorRef[Consumer.Event]],
      override val activeProducers: Map[String, ActorRef[Producer.Event]],
      override val pendingLocalPublications: Seq[(String, PublishReceivedLocally)],
      override val pendingRemotePublications: Seq[(String, PublishReceivedFromRemote)],
      override val consumerPacketRouter: ActorRef[RemotePacketRouter.Request[Consumer.Event]],
      override val producerPacketRouter: ActorRef[LocalPacketRouter.Request[Producer.Event]],
      override val publisherPacketRouter: ActorRef[RemotePacketRouter.Request[Publisher.Event]],
      override val unpublisherPacketRouter: ActorRef[RemotePacketRouter.Request[Unpublisher.Event]],
      override val settings: MqttSessionSettings
  ) extends Data(
        stash,
        publishers,
        activeConsumers,
        activeProducers,
        pendingLocalPublications,
        pendingRemotePublications,
        consumerPacketRouter,
        producerPacketRouter,
        publisherPacketRouter,
        unpublisherPacketRouter,
        settings
      )
  final case class ConnAckReplied(
      connect: Connect,
      remote: SourceQueueWithComplete[ForwardConnAckCommand],
      override val stash: Seq[Event],
      override val publishers: Set[String],
      override val activeConsumers: Map[String, ActorRef[Consumer.Event]],
      override val activeProducers: Map[String, ActorRef[Producer.Event]],
      override val pendingLocalPublications: Seq[(String, PublishReceivedLocally)],
      override val pendingRemotePublications: Seq[(String, PublishReceivedFromRemote)],
      override val consumerPacketRouter: ActorRef[RemotePacketRouter.Request[Consumer.Event]],
      override val producerPacketRouter: ActorRef[LocalPacketRouter.Request[Producer.Event]],
      override val publisherPacketRouter: ActorRef[RemotePacketRouter.Request[Publisher.Event]],
      override val unpublisherPacketRouter: ActorRef[RemotePacketRouter.Request[Unpublisher.Event]],
      override val settings: MqttSessionSettings
  ) extends Data(
        stash,
        publishers,
        activeConsumers,
        activeProducers,
        pendingLocalPublications,
        pendingRemotePublications,
        consumerPacketRouter,
        producerPacketRouter,
        publisherPacketRouter,
        unpublisherPacketRouter,
        settings
      )
  final case class Disconnected(
      override val stash: Seq[Event],
      override val publishers: Set[String],
      override val activeConsumers: Map[String, ActorRef[Consumer.Event]],
      override val activeProducers: Map[String, ActorRef[Producer.Event]],
      override val pendingLocalPublications: Seq[(String, PublishReceivedLocally)],
      override val pendingRemotePublications: Seq[(String, PublishReceivedFromRemote)],
      override val consumerPacketRouter: ActorRef[RemotePacketRouter.Request[Consumer.Event]],
      override val producerPacketRouter: ActorRef[LocalPacketRouter.Request[Producer.Event]],
      override val publisherPacketRouter: ActorRef[RemotePacketRouter.Request[Publisher.Event]],
      override val unpublisherPacketRouter: ActorRef[RemotePacketRouter.Request[Unpublisher.Event]],
      override val settings: MqttSessionSettings
  ) extends Data(
        stash,
        publishers,
        activeConsumers,
        activeProducers,
        pendingLocalPublications,
        pendingRemotePublications,
        consumerPacketRouter,
        producerPacketRouter,
        publisherPacketRouter,
        unpublisherPacketRouter,
        settings
      )

  sealed abstract class Event
  case object ReceiveConnAckTimeout extends Event
  final case class ConnAckReceivedLocally(connAck: ConnAck, remote: Promise[Source[ForwardConnAckCommand, NotUsed]])
      extends Event
  final case class SubscribeReceivedFromRemote(subscribe: Subscribe, local: Promise[Publisher.ForwardSubscribe.type])
      extends Event
  final case class Subscribed(subscribe: Subscribe, remote: Promise[Publisher.ForwardSubAck.type]) extends Event
  final case class PublishReceivedFromRemote(publish: Publish, local: Promise[Consumer.ForwardPublish.type])
      extends Event
  final case class ConsumerFree(topicName: String) extends Event
  final case class PublishReceivedLocally(publish: Publish, publishData: Producer.PublishData) extends Event
  final case class ProducerFree(topicName: String) extends Event
  final case class UnsubscribeReceivedFromRemote(unsubscribe: Unsubscribe,
                                                 local: Promise[Unpublisher.ForwardUnsubscribe.type])
      extends Event
  final case class Unsubscribed(unsubscribe: Unsubscribe) extends Event
  final case class PingReqReceivedFromRemote(local: Promise[ForwardPingReq.type]) extends Event
  final case class DisconnectReceivedFromRemote(local: Promise[ForwardDisconnect.type]) extends Event
  case object ConnectionLost extends Event
  case object ReceivePingReqTimeout extends Event
  final case class ReceivedProducerPublishingCommand(command: Producer.ForwardPublishingCommand) extends Event
  final case class ConnectReceivedFromRemote(connect: Connect, local: Promise[ClientConnection.ForwardConnect.type])
      extends Event
  case object ReceiveConnectTimeout extends Event
  final case class QueueOfferCompleted(result: Either[Throwable, QueueOfferResult])
      extends Event
      with QueueOfferState.QueueOfferCompleted

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

  def clientConnect(data: ConnectReceived)(implicit mat: Materializer): Behavior[Event] = Behaviors.setup { _ =>
    data.local.trySuccess(ForwardConnect)

    Behaviors.withTimers { timer =>
      val ReceiveConnAck = "receive-connack"
      if (!timer.isTimerActive(ReceiveConnAck))
        timer.startSingleTimer(ReceiveConnAck, ReceiveConnAckTimeout, data.settings.receiveConnAckTimeout)

      Behaviors
        .receivePartial[Event] {
          case (context, ConnAckReceivedLocally(_, remote)) =>
            import context.executionContext

            val (queue, source) = Source
              .queue[ForwardConnAckCommand](data.settings.serverSendBufferSize, OverflowStrategy.backpressure)
              .toMat(BroadcastHub.sink)(Keep.both)
              .run()

            remote.success(source)

            queue
              .offer(ForwardConnAck)
              .onComplete(result => context.self.tell(QueueOfferCompleted(result.toEither)))

            timer.cancel(ReceiveConnAck)

            data.activeProducers.values
              .foreach(_ ! Producer.ReceiveConnect)

            QueueOfferState.waitForQueueOfferCompleted(
              clientConnected(
                ConnAckReplied(
                  data.connect,
                  queue,
                  Vector.empty,
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
              ),
              stash = data.stash
            )

          case (_, ReceiveConnAckTimeout) =>
            throw ClientConnectionFailed
          case (_, ClientConnection.ConnectionLost) =>
            throw ClientConnectionFailed
          case (_, PublishReceivedLocally(publish, _))
              if !data.publishers.exists(Topics.filter(_, publish.topicName)) =>
            Behaviors.same
          case (_, e) =>
            clientConnect(data.copy(stash = data.stash :+ e))
        }
        .receiveSignal {
          case (_, _: Terminated) =>
            Behaviors.same
        }
    }
  }

  def disconnect(context: ActorContext[Event], remote: SourceQueueWithComplete[ForwardConnAckCommand], data: Data)(
      implicit mat: Materializer
  ): Behavior[Event] = {
    remote.complete()

    data.stash.foreach(context.self.tell)

    clientDisconnected(
      Disconnected(
        Vector.empty,
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
  }

  def clientConnected(data: ConnAckReplied)(implicit mat: Materializer): Behavior[Event] = Behaviors.withTimers {
    timer =>
      val ReceivePingreq = "receive-pingreq"
      if (data.connect.keepAlive.toMillis > 0)
        timer.startSingleTimer(ReceivePingreq,
                               ReceivePingReqTimeout,
                               FiniteDuration((data.connect.keepAlive.toMillis * 1.5).toLong, TimeUnit.MILLISECONDS))

      Behaviors
        .receivePartial[Event] {
          case (context, SubscribeReceivedFromRemote(subscribe, local)) =>
            val subscribed = Promise[Promise[Publisher.ForwardSubAck.type]]
            context.watch(
              context.spawnAnonymous(
                Publisher(data.connect.clientId,
                          subscribe.packetId,
                          local,
                          subscribed,
                          data.publisherPacketRouter,
                          data.settings)
              )
            )
            subscribed.future.foreach(remote => context.self ! Subscribed(subscribe, remote))(context.executionContext)
            clientConnected(data)
          case (_, Subscribed(subscribe, remote)) =>
            remote.success(Publisher.ForwardSubAck)

            clientConnected(
              data.copy(
                publishers = data.publishers ++ subscribe.topicFilters.map(_._1)
              )
            )
          case (context, UnsubscribeReceivedFromRemote(unsubscribe, local)) =>
            val unsubscribed = Promise[Done]
            context.watch(
              context.spawnAnonymous(
                Unpublisher(data.connect.clientId,
                            unsubscribe.packetId,
                            local,
                            unsubscribed,
                            data.unpublisherPacketRouter,
                            data.settings)
              )
            )
            unsubscribed.future.foreach(_ => context.self ! Unsubscribed(unsubscribe))(context.executionContext)
            clientConnected(data)
          case (_, Unsubscribed(unsubscribe)) =>
            clientConnected(data.copy(publishers = data.publishers -- unsubscribe.topicFilters))
          case (_, PublishReceivedFromRemote(publish, local))
              if (publish.flags & ControlPacketFlags.QoSReserved).underlying == 0 =>
            local.success(Consumer.ForwardPublish)
            clientConnected(data)
          case (context, prfr @ PublishReceivedFromRemote(publish @ Publish(_, topicName, Some(packetId), _), local)) =>
            data.activeConsumers.get(topicName) match {
              case None =>
                val consumerName = ActorName.mkName(ConsumerNamePrefix + topicName + "-" + context.children.size)
                val consumer =
                  context.spawn(Consumer(publish,
                                         Some(data.connect.clientId),
                                         packetId,
                                         local,
                                         data.consumerPacketRouter,
                                         data.settings),
                                consumerName)
                context.watchWith(consumer, ConsumerFree(publish.topicName))
                clientConnected(data.copy(activeConsumers = data.activeConsumers + (publish.topicName -> consumer)))
              case Some(consumer) if publish.flags.contains(ControlPacketFlags.DUP) =>
                consumer ! Consumer.DupPublishReceivedFromRemote(local)
                clientConnected(data)
              case Some(_) =>
                clientConnected(
                  data.copy(pendingRemotePublications = data.pendingRemotePublications :+ (publish.topicName -> prfr))
                )
            }
          case (context, ConsumerFree(topicName)) =>
            val i = data.pendingRemotePublications.indexWhere(_._1 == topicName)
            if (i >= 0) {
              val prfr = data.pendingRemotePublications(i)._2
              val consumerName = ActorName.mkName(ConsumerNamePrefix + topicName + "-" + context.children.size)
              val consumer = context.spawn(
                Consumer(prfr.publish,
                         Some(data.connect.clientId),
                         prfr.publish.packetId.get,
                         prfr.local,
                         data.consumerPacketRouter,
                         data.settings),
                consumerName
              )
              context.watchWith(
                consumer,
                ConsumerFree(topicName)
              )
              clientConnected(
                data.copy(
                  activeConsumers = data.activeConsumers + (topicName -> consumer),
                  pendingRemotePublications =
                    data.pendingRemotePublications.take(i) ++ data.pendingRemotePublications.drop(i + 1)
                )
              )
            } else {
              clientConnected(data.copy(activeConsumers = data.activeConsumers - topicName))
            }
          case (context, PublishReceivedLocally(publish, _))
              if (publish.flags & ControlPacketFlags.QoSReserved).underlying == 0 &&
              data.publishers.exists(Topics.filter(_, publish.topicName)) =>
            import context.executionContext

            data.remote
              .offer(ForwardPublish(publish, None))
              .onComplete(result => context.self.tell(QueueOfferCompleted(result.toEither)))

            QueueOfferState.waitForQueueOfferCompleted(
              clientConnected(data),
              stash = Seq.empty
            )

          case (context, prl @ PublishReceivedLocally(publish, publishData))
              if data.publishers.exists(Topics.filter(_, publish.topicName)) =>
            val producerName = ActorName.mkName(ProducerNamePrefix + publish.topicName + "-" + context.children.size)
            if (!data.activeProducers.contains(publish.topicName)) {
              val reply = Promise[Source[Producer.ForwardPublishingCommand, NotUsed]]
              import context.executionContext
              reply.future.foreach {
                _.runForeach(command => context.self ! ReceivedProducerPublishingCommand(command))
              }
              val producer =
                context.spawn(Producer(publish, publishData, reply, data.producerPacketRouter, data.settings),
                              producerName)
              context.watchWith(
                producer,
                ProducerFree(publish.topicName)
              )
              clientConnected(data.copy(activeProducers = data.activeProducers + (publish.topicName -> producer)))
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
              reply.future.foreach {
                _.runForeach(command => context.self ! ReceivedProducerPublishingCommand(command))
              }
              val producer = context.spawn(
                Producer(prl.publish, prl.publishData, reply, data.producerPacketRouter, data.settings),
                producerName
              )
              context.watchWith(
                producer,
                ProducerFree(topicName)
              )
              clientConnected(
                data.copy(
                  activeProducers = data.activeProducers + (topicName -> producer),
                  pendingLocalPublications =
                    data.pendingLocalPublications.take(i) ++ data.pendingLocalPublications.drop(i + 1)
                )
              )
            } else {
              clientConnected(data.copy(activeProducers = data.activeProducers - topicName))
            }
          case (context, ReceivedProducerPublishingCommand(command)) =>
            import context.executionContext

            command match {
              case Producer.ForwardPublish(publish, packetId) =>
                data.remote
                  .offer(ForwardPublish(publish, packetId))
                  .onComplete(result => context.self.tell(QueueOfferCompleted(result.toEither)))
              case Producer.ForwardPubRel(_, packetId) =>
                data.remote
                  .offer(ForwardPubRel(packetId))
                  .onComplete(result => context.self.tell(QueueOfferCompleted(result.toEither)))
            }

            QueueOfferState.waitForQueueOfferCompleted(
              clientConnected(data),
              stash = Seq.empty
            )
          case (context, PingReqReceivedFromRemote(local)) =>
            import context.executionContext

            data.remote
              .offer(ForwardPingResp)
              .onComplete(result => context.self.tell(QueueOfferCompleted(result.toEither)))

            local.success(ForwardPingReq)

            QueueOfferState.waitForQueueOfferCompleted(
              clientConnected(data),
              stash = Seq.empty
            )

          case (context, ReceivePingReqTimeout) =>
            data.remote.fail(ServerConnector.PingFailed)
            timer.cancel(ReceivePingreq)
            disconnect(context, data.remote, data)
          case (context, DisconnectReceivedFromRemote(local)) =>
            local.success(ForwardDisconnect)
            timer.cancel(ReceivePingreq)
            disconnect(context, data.remote, data)
          case (context, ClientConnection.ConnectionLost) =>
            timer.cancel(ReceivePingreq)
            disconnect(context, data.remote, data)
          case (context, ConnectReceivedFromRemote(connect, local))
              if connect.connectFlags.contains(ConnectFlags.CleanSession) =>
            context.children.foreach(context.stop)
            timer.cancel(ReceivePingreq)
            data.remote.complete()
            clientConnect(
              ConnectReceived(
                connect,
                local,
                Vector.empty,
                Set.empty,
                Map.empty,
                Map.empty,
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
            timer.cancel(ReceivePingreq)
            data.remote.complete()
            clientConnect(
              ConnectReceived(
                connect,
                local,
                Vector.empty,
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
        }
        .receiveSignal {
          case (context, ChildFailed(_, failure))
              if failure == Publisher.SubscribeFailed || failure == Unpublisher.UnsubscribeFailed =>
            disconnect(context, data.remote, data)
          case (_, _: Terminated) =>
            Behaviors.same
          case (_, PostStop) =>
            data.remote.complete()
            Behaviors.same
        }
  }

  def clientDisconnected(data: Disconnected)(implicit mat: Materializer): Behavior[Event] = Behaviors.withTimers {
    timer =>
      val ReceiveConnect = "receive-connect"
      if (!timer.isTimerActive(ReceiveConnect))
        timer.startSingleTimer(ReceiveConnect, ReceiveConnectTimeout, data.settings.receiveConnectTimeout)

      Behaviors
        .receivePartial[Event] {
          case (context, ConnectReceivedFromRemote(connect, local))
              if connect.connectFlags.contains(ConnectFlags.CleanSession) =>
            context.children.foreach(context.stop)
            timer.cancel(ReceiveConnect)
            clientConnect(
              ConnectReceived(
                connect,
                local,
                Vector.empty,
                Set.empty,
                Map.empty,
                Map.empty,
                Vector.empty,
                Vector.empty,
                data.consumerPacketRouter,
                data.producerPacketRouter,
                data.publisherPacketRouter,
                data.unpublisherPacketRouter,
                data.settings
              )
            )
          case (context, ConnectReceivedFromRemote(connect, local)) =>
            timer.cancel(ReceiveConnect)
            data.stash.foreach(context.self.tell)
            clientConnect(
              ConnectReceived(
                connect,
                local,
                Vector.empty,
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
          case (_, ReceiveConnectTimeout) =>
            throw ClientConnectionFailed
          case (_, ConnectionLost) =>
            Behavior.same // We know... we are disconnected...
          case (_, PublishReceivedLocally(publish, _))
              if !data.publishers.exists(Topics.filter(_, publish.topicName)) =>
            Behaviors.same
          case (_, e) =>
            clientDisconnected(data.copy(stash = data.stash :+ e))
        }
        .receiveSignal {
          case (_, _: Terminated) =>
            Behaviors.same
        }
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
            subscribed: Promise[Promise[ForwardSubAck.type]],
            packetRouter: ActorRef[RemotePacketRouter.Request[Event]],
            settings: MqttSessionSettings): Behavior[Event] =
    preparePublisher(Start(Some(clientId), packetId, local, subscribed, packetRouter, settings))

  // Our FSM data, FSM events and commands emitted by the FSM

  sealed abstract class Data(val clientId: Some[String],
                             val packetId: PacketId,
                             val subscribed: Promise[Promise[ForwardSubAck.type]],
                             val packetRouter: ActorRef[RemotePacketRouter.Request[Event]],
                             val settings: MqttSessionSettings)
  final case class Start(override val clientId: Some[String],
                         override val packetId: PacketId,
                         local: Promise[ForwardSubscribe.type],
                         override val subscribed: Promise[Promise[ForwardSubAck.type]],
                         override val packetRouter: ActorRef[RemotePacketRouter.Request[Event]],
                         override val settings: MqttSessionSettings)
      extends Data(clientId, packetId, subscribed, packetRouter, settings)
  final case class ServerSubscribe(override val clientId: Some[String],
                                   override val packetId: PacketId,
                                   override val subscribed: Promise[Promise[ForwardSubAck.type]],
                                   override val packetRouter: ActorRef[RemotePacketRouter.Request[Event]],
                                   override val settings: MqttSessionSettings)
      extends Data(clientId, packetId, subscribed, packetRouter, settings)

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
        serverSubscribe(
          ServerSubscribe(data.clientId, data.packetId, data.subscribed, data.packetRouter, data.settings)
        )
      case UnobtainablePacketId =>
        data.local.failure(SubscribeFailed)
        data.subscribed.failure(SubscribeFailed)
        throw SubscribeFailed
    }

  }

  def serverSubscribe(data: ServerSubscribe): Behavior[Event] = Behaviors.withTimers { timer =>
    val ReceiveSuback = "server-receive-suback"
    timer.startSingleTimer(ReceiveSuback, ReceiveSubAckTimeout, data.settings.receiveSubAckTimeout)

    Behaviors
      .receiveMessagePartial[Event] {
        case SubAckReceivedLocally(remote) =>
          data.subscribed.success(remote)
          Behaviors.stopped
        case ReceiveSubAckTimeout =>
          data.subscribed.failure(SubscribeFailed)
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
            unsubscribed: Promise[Done],
            packetRouter: ActorRef[RemotePacketRouter.Request[Event]],
            settings: MqttSessionSettings): Behavior[Event] =
    prepareServerUnpublisher(Start(Some(clientId), packetId, local, unsubscribed, packetRouter, settings))

  // Our FSM data, FSM events and commands emitted by the FSM

  sealed abstract class Data(val clientId: Some[String],
                             val packetId: PacketId,
                             val unsubscribed: Promise[Done],
                             val packetRouter: ActorRef[RemotePacketRouter.Request[Event]],
                             val settings: MqttSessionSettings)
  final case class Start(override val clientId: Some[String],
                         override val packetId: PacketId,
                         local: Promise[ForwardUnsubscribe.type],
                         override val unsubscribed: Promise[Done],
                         override val packetRouter: ActorRef[RemotePacketRouter.Request[Event]],
                         override val settings: MqttSessionSettings)
      extends Data(clientId, packetId, unsubscribed, packetRouter, settings)
  final case class ServerUnsubscribe(override val clientId: Some[String],
                                     override val packetId: PacketId,
                                     override val unsubscribed: Promise[Done],
                                     override val packetRouter: ActorRef[RemotePacketRouter.Request[Event]],
                                     override val settings: MqttSessionSettings)
      extends Data(clientId, packetId, unsubscribed, packetRouter, settings)

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
        serverUnsubscribe(
          ServerUnsubscribe(data.clientId, data.packetId, data.unsubscribed, data.packetRouter, data.settings)
        )
      case UnobtainablePacketId =>
        data.local.failure(UnsubscribeFailed)
        data.unsubscribed.failure(UnsubscribeFailed)
        throw UnsubscribeFailed
    }
  }

  def serverUnsubscribe(data: ServerUnsubscribe): Behavior[Event] = Behaviors.withTimers { timer =>
    val ReceiveUnsubAck = "server-receive-unsubAck"
    timer.startSingleTimer(ReceiveUnsubAck, ReceiveUnsubAckTimeout, data.settings.receiveUnsubAckTimeout)

    Behaviors
      .receiveMessagePartial[Event] {
        case UnsubAckReceivedLocally(remote) =>
          remote.success(ForwardUnsubAck)
          data.unsubscribed.success(Done)
          Behaviors.stopped
        case ReceiveUnsubAckTimeout =>
          data.unsubscribed.failure(UnsubscribeFailed)
          throw UnsubscribeFailed
      }
      .receiveSignal {
        case (_, PostStop) =>
          data.packetRouter ! RemotePacketRouter.Unregister(data.clientId, data.packetId)
          Behaviors.same
      }
  }
}
