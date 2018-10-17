/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.mqtt.streaming
package impl

import java.util.concurrent.TimeUnit

import akka.NotUsed
import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.{ActorRef, Behavior, PostStop, Terminated}
import akka.annotation.InternalApi
import akka.stream.{Materializer, OverflowStrategy}
import akka.stream.scaladsl.{BroadcastHub, Keep, Sink, Source, SourceQueueWithComplete}
import akka.stream.typed.scaladsl.ActorMaterializer
import akka.util.{ByteString, Timeout}

import scala.annotation.tailrec
import scala.concurrent.duration.FiniteDuration
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
  case object PingFailed extends Exception

  /*
   * Construct with the starting state
   */
  def apply(consumerPacketRouter: ActorRef[RemotePacketRouter.Request[Consumer.Event]],
            producerPacketRouter: ActorRef[LocalPacketRouter.Request[Producer.Event]],
            publisherPacketRouter: ActorRef[RemotePacketRouter.Request[Publisher.Event]],
            unpublisherPacketRouter: ActorRef[RemotePacketRouter.Request[Unpublisher.Event]],
            settings: MqttSessionSettings): Behavior[Event] =
    listening(
      Listening(Map.empty,
                consumerPacketRouter,
                producerPacketRouter,
                publisherPacketRouter,
                unpublisherPacketRouter,
                settings)
    )

  // Our FSM data, FSM events and commands emitted by the FSM

  sealed abstract class Data(val consumerPacketRouter: ActorRef[RemotePacketRouter.Request[Consumer.Event]],
                             val producerPacketRouter: ActorRef[LocalPacketRouter.Request[Producer.Event]],
                             val publisherPacketRouter: ActorRef[RemotePacketRouter.Request[Publisher.Event]],
                             val unpublisherPacketRouter: ActorRef[RemotePacketRouter.Request[Unpublisher.Event]],
                             val settings: MqttSessionSettings)
  final case class Listening(
      clientConnections: Map[ByteString, ActorRef[ClientConnection.Event]],
      override val consumerPacketRouter: ActorRef[RemotePacketRouter.Request[Consumer.Event]],
      override val producerPacketRouter: ActorRef[LocalPacketRouter.Request[Producer.Event]],
      override val publisherPacketRouter: ActorRef[RemotePacketRouter.Request[Publisher.Event]],
      override val unpublisherPacketRouter: ActorRef[RemotePacketRouter.Request[Unpublisher.Event]],
      override val settings: MqttSessionSettings
  ) extends Data(consumerPacketRouter, producerPacketRouter, publisherPacketRouter, unpublisherPacketRouter, settings)
  final case class Accepting(
      stash: Seq[Event],
      override val consumerPacketRouter: ActorRef[RemotePacketRouter.Request[Consumer.Event]],
      override val producerPacketRouter: ActorRef[LocalPacketRouter.Request[Producer.Event]],
      override val publisherPacketRouter: ActorRef[RemotePacketRouter.Request[Publisher.Event]],
      override val unpublisherPacketRouter: ActorRef[RemotePacketRouter.Request[Unpublisher.Event]],
      override val settings: MqttSessionSettings
  ) extends Data(consumerPacketRouter, producerPacketRouter, publisherPacketRouter, unpublisherPacketRouter, settings)

  sealed abstract class Event(val connectionId: ByteString)
  final case class ConnectReceivedFromRemote(override val connectionId: ByteString,
                                             connect: Connect,
                                             local: ActorRef[ClientConnection.ForwardConnect.type])
      extends Event(connectionId)
  final case class ReceiveConnAckTimeout(override val connectionId: ByteString) extends Event(connectionId)
  final case class ConnAckReceivedLocally(override val connectionId: ByteString,
                                          connAck: ConnAck,
                                          remote: ActorRef[Source[ClientConnection.ForwardConnAckCommand, NotUsed]])
      extends Event(connectionId)
  final case class SubscribeReceivedFromRemote(override val connectionId: ByteString,
                                               subscribe: Subscribe,
                                               local: ActorRef[Publisher.ForwardSubscribe.type])
      extends Event(connectionId)
  final case class PublishReceivedFromRemote(override val connectionId: ByteString,
                                             publish: Publish,
                                             local: ActorRef[Consumer.ForwardPublish.type])
      extends Event(connectionId)
  final case class PublishReceivedLocally(override val connectionId: ByteString,
                                          publish: Publish,
                                          publishData: Producer.PublishData)
      extends Event(connectionId)
  final case class UnsubscribeReceivedFromRemote(override val connectionId: ByteString,
                                                 unsubscribe: Unsubscribe,
                                                 local: ActorRef[Unpublisher.ForwardUnsubscribe.type])
      extends Event(connectionId)
  final case class PingReqReceivedFromRemote(override val connectionId: ByteString,
                                             local: ActorRef[ClientConnection.ForwardPingReq.type])
      extends Event(connectionId)
  final case class DisconnectReceivedFromRemote(override val connectionId: ByteString,
                                                local: ActorRef[ClientConnection.ForwardDisconnect.type])
      extends Event(connectionId)
  final case class ConnectionLost(override val connectionId: ByteString) extends Event(connectionId)
  final case class ClientConnectionFree(override val connectionId: ByteString) extends Event(connectionId)

  // State event handling

  private val ClientConnectionNamePrefix = "client-connection-"

  private def forward(connectionId: ByteString,
                      clientConnections: Map[ByteString, ActorRef[ClientConnection.Event]],
                      e: ClientConnection.Event): Behavior[Event] = {
    clientConnections.get(connectionId).foreach(_ ! e)
    Behaviors.same
  }

  def listening(data: Listening): Behavior[Event] = Behaviors.receivePartial {
    case (context, ConnectReceivedFromRemote(connectionId, connect, local)) =>
      val clientConnectionName = ActorName.mkName(ClientConnectionNamePrefix + connectionId)
      context.child(clientConnectionName) match {
        case None =>
          val clientConnection = context.spawn(
            ClientConnection(local,
                             connect.keepAlive,
                             data.consumerPacketRouter,
                             data.producerPacketRouter,
                             data.publisherPacketRouter,
                             data.unpublisherPacketRouter,
                             data.settings),
            clientConnectionName
          )
          context.watchWith(clientConnection, ClientConnectionFree(connectionId))
          listening(data.copy(clientConnections = data.clientConnections + (connectionId -> clientConnection)))
        case _: Some[_] => // Ignored for existing subscriptions
          Behaviors.same
      }
    case (_, ClientConnectionFree(connectionId)) =>
      listening(data.copy(clientConnections = data.clientConnections - connectionId))
    case (context, ConnectionLost(connectionId)) =>
      data.clientConnections.get(connectionId).foreach(context.stop)
      listening(data.copy(clientConnections = data.clientConnections - connectionId))
    case (_, ConnAckReceivedLocally(connectionId, connAck, remote)) =>
      forward(connectionId, data.clientConnections, ClientConnection.ConnAckReceivedLocally(connAck, remote))
    case (_, DisconnectReceivedFromRemote(connectionId, local)) =>
      forward(connectionId, data.clientConnections, ClientConnection.DisconnectReceivedFromRemote(local))
    case (_, SubscribeReceivedFromRemote(connectionId, subscribe, local)) =>
      forward(connectionId, data.clientConnections, ClientConnection.SubscribeReceivedFromRemote(subscribe, local))
    case (_, PublishReceivedFromRemote(connectionId, publish, local)) =>
      forward(connectionId, data.clientConnections, ClientConnection.PublishReceivedFromRemote(publish, local))
    case (_, PublishReceivedLocally(_, publish, publishData)) =>
      data.clientConnections.values.foreach(_ ! ClientConnection.PublishReceivedLocally(publish, publishData))
      Behaviors.same
    case (_, UnsubscribeReceivedFromRemote(connectionId, unsubscribe, local)) =>
      forward(connectionId, data.clientConnections, ClientConnection.UnsubscribeReceivedFromRemote(unsubscribe, local))
    case (_, PingReqReceivedFromRemote(connectionId, local)) =>
      forward(connectionId, data.clientConnections, ClientConnection.PingReqReceivedFromRemote(local))
  }
}

/*
 * Handles events in relation to a specific client connection
 */
@InternalApi private[streaming] object ClientConnection {

  /*
   * A PINGREQ was not received within 1.5 times the keep alive so the connection will close
   *
   * http://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html
   * 3.1.2.10 Keep Alive
   */
  case object PingFailed extends Exception

  /*
   * Construct with the starting state
   */
  def apply(local: ActorRef[ForwardConnect.type],
            keepAlive: FiniteDuration,
            consumerPacketRouter: ActorRef[RemotePacketRouter.Request[Consumer.Event]],
            producerPacketRouter: ActorRef[LocalPacketRouter.Request[Producer.Event]],
            publisherPacketRouter: ActorRef[RemotePacketRouter.Request[Publisher.Event]],
            unpublisherPacketRouter: ActorRef[RemotePacketRouter.Request[Unpublisher.Event]],
            settings: MqttSessionSettings): Behavior[Event] =
    clientConnect(
      ConnectReceived(local,
                      Vector.empty,
                      keepAlive,
                      consumerPacketRouter,
                      producerPacketRouter,
                      publisherPacketRouter,
                      unpublisherPacketRouter,
                      settings)
    )

  // Our FSM data, FSM events and commands emitted by the FSM

  sealed abstract class Data(val keepAlive: FiniteDuration,
                             val consumerPacketRouter: ActorRef[RemotePacketRouter.Request[Consumer.Event]],
                             val producerPacketRouter: ActorRef[LocalPacketRouter.Request[Producer.Event]],
                             val publisherPacketRouter: ActorRef[RemotePacketRouter.Request[Publisher.Event]],
                             val unpublisherPacketRouter: ActorRef[RemotePacketRouter.Request[Unpublisher.Event]],
                             val settings: MqttSessionSettings)
  final case class ConnectReceived(
      local: ActorRef[ForwardConnect.type],
      stash: Seq[Event],
      override val keepAlive: FiniteDuration,
      override val consumerPacketRouter: ActorRef[RemotePacketRouter.Request[Consumer.Event]],
      override val producerPacketRouter: ActorRef[LocalPacketRouter.Request[Producer.Event]],
      override val publisherPacketRouter: ActorRef[RemotePacketRouter.Request[Publisher.Event]],
      override val unpublisherPacketRouter: ActorRef[RemotePacketRouter.Request[Unpublisher.Event]],
      override val settings: MqttSessionSettings
  ) extends Data(keepAlive,
                   consumerPacketRouter,
                   producerPacketRouter,
                   publisherPacketRouter,
                   unpublisherPacketRouter,
                   settings)
  final case class ConnAckReplied(
      remote: SourceQueueWithComplete[ForwardConnAckCommand],
      publishers: Set[String],
      pendingLocalPublications: Seq[(String, PublishReceivedLocally)],
      stash: Seq[Event],
      override val keepAlive: FiniteDuration,
      override val consumerPacketRouter: ActorRef[RemotePacketRouter.Request[Consumer.Event]],
      override val producerPacketRouter: ActorRef[LocalPacketRouter.Request[Producer.Event]],
      override val publisherPacketRouter: ActorRef[RemotePacketRouter.Request[Publisher.Event]],
      override val unpublisherPacketRouter: ActorRef[RemotePacketRouter.Request[Unpublisher.Event]],
      override val settings: MqttSessionSettings
  ) extends Data(keepAlive,
                   consumerPacketRouter,
                   producerPacketRouter,
                   publisherPacketRouter,
                   unpublisherPacketRouter,
                   settings)

  sealed abstract class Event
  case object ReceiveConnAckTimeout extends Event
  final case class ConnAckReceivedLocally(connAck: ConnAck, remote: ActorRef[Source[ForwardConnAckCommand, NotUsed]])
      extends Event
  final case class SubscribeReceivedFromRemote(subscribe: Subscribe, local: ActorRef[Publisher.ForwardSubscribe.type])
      extends Event
  final case class PublishReceivedFromRemote(publish: Publish, local: ActorRef[Consumer.ForwardPublish.type])
      extends Event
  final case class PublishReceivedLocally(publish: Publish, publishData: Producer.PublishData) extends Event
  final case class ProducerFree(topicName: String) extends Event
  final case class UnsubscribeReceivedFromRemote(unsubscribe: Unsubscribe,
                                                 local: ActorRef[Unpublisher.ForwardUnsubscribe.type])
      extends Event
  final case class PingReqReceivedFromRemote(local: ActorRef[ForwardPingReq.type]) extends Event
  final case class DisconnectReceivedFromRemote(local: ActorRef[ForwardDisconnect.type]) extends Event
  case object ConnectionLost extends Event
  final case class PublisherFree(topicFilters: Seq[(String, ControlPacketFlags)]) extends Event
  final case class UnpublisherFree(topicFilters: Seq[String]) extends Event
  case object ReceivePingReqTimeout extends Event
  final case class ReceivedProducerPublishingCommand(command: Source[Producer.ForwardPublishingCommand, NotUsed])
      extends Event

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

  def clientConnect(data: ConnectReceived): Behavior[Event] = Behaviors.setup { _ =>
    data.local ! ForwardConnect

    Behaviors.withTimers { timer =>
      timer.startSingleTimer("receive-connack", ReceiveConnAckTimeout, data.settings.receiveConnAckTimeout)

      Behaviors
        .receivePartial[Event] {
          case (context, ConnAckReceivedLocally(_, remote)) =>
            implicit val mat: Materializer = ActorMaterializer()(context.system)
            val (queue, source) = Source
              .queue[ForwardConnAckCommand](data.settings.serverSendBufferSize, OverflowStrategy.dropNew)
              .toMat(BroadcastHub.sink)(Keep.both)
              .run()
            remote ! source

            queue.offer(ForwardConnAck)
            data.stash.foreach(context.self.tell)

            clientConnected(
              ConnAckReplied(
                queue,
                Set.empty,
                Vector.empty,
                Vector.empty,
                data.keepAlive,
                data.consumerPacketRouter,
                data.producerPacketRouter,
                data.publisherPacketRouter,
                data.unpublisherPacketRouter,
                data.settings
              )
            )
          case (_, ReceiveConnAckTimeout) =>
            Behaviors.stopped
          case (_, e) if data.stash.size < data.settings.maxClientConnectionStashSize =>
            clientConnect(data.copy(stash = data.stash :+ e))
        }
    }
  }

  def clientConnected(data: ConnAckReplied): Behavior[Event] = Behaviors.setup { setupContext =>
    val selfAsRemote =
      setupContext.messageAdapter[Source[Producer.ForwardPublishingCommand, NotUsed]](ReceivedProducerPublishingCommand)

    Behaviors.withTimers { timer =>
      if (data.keepAlive.toMillis > 0)
        timer.startSingleTimer("receive-pingreq",
                               ReceivePingReqTimeout,
                               FiniteDuration((data.keepAlive.toMillis * 1.5).toLong, TimeUnit.MILLISECONDS))

      Behaviors
        .receivePartial[Event] {
          case (_, DisconnectReceivedFromRemote(local)) =>
            local ! ForwardDisconnect
            Behaviors.stopped
          case (context, SubscribeReceivedFromRemote(subscribe, local)) =>
            val publisherName = ActorName.mkName(PublisherNamePrefix + subscribe.topicFilters)
            context.child(publisherName) match {
              case None =>
                val publisher = context.spawn(
                  Publisher(subscribe.packetId, local, data.publisherPacketRouter, data.settings),
                  publisherName
                )
                context.watchWith(publisher, PublisherFree(subscribe.topicFilters))
                pendingSubAck(data)
              case _: Some[_] => // Ignored for existing subscriptions
                Behaviors.same
            }
          case (context, UnsubscribeReceivedFromRemote(unsubscribe, local)) =>
            val unpublisherName = ActorName.mkName(UnpublisherNamePrefix + unsubscribe.topicFilters)
            context.child(unpublisherName) match {
              case None =>
                val unpublisher = context.spawn(
                  Unpublisher(unsubscribe.packetId, local, data.unpublisherPacketRouter, data.settings),
                  unpublisherName
                )
                context.watchWith(unpublisher, UnpublisherFree(unsubscribe.topicFilters))
              case _: Some[_] => // Ignored for existing unsubscriptions
            }
            Behaviors.same
          case (_, UnpublisherFree(topicFilters)) =>
            val unsubscribedTopicFilters =
              data.publishers.filter(publisher => topicFilters.exists(matchTopicFilter(_, publisher)))
            clientConnected(data.copy(publishers = data.publishers -- unsubscribedTopicFilters))
          case (_, PublishReceivedFromRemote(publish, local))
              if (publish.flags & ControlPacketFlags.QoSReserved).underlying == 0 =>
            local ! Consumer.ForwardPublish
            Behaviors.same
          case (context, PublishReceivedFromRemote(publish @ Publish(_, topicName, Some(packetId), _), local)) =>
            val consumerName = ActorName.mkName(ConsumerNamePrefix + topicName + packetId.underlying)
            context.child(consumerName) match {
              case None =>
                context.spawn(Consumer(publish, packetId, local, data.consumerPacketRouter, data.settings),
                              consumerName)
              case _: Some[_] => // Ignored for existing consumptions
            }
            Behaviors.same
          case (_, PublishReceivedLocally(publish, _))
              if (publish.flags & ControlPacketFlags.QoSReserved).underlying == 0 &&
              data.publishers.exists(matchTopicFilter(_, publish.topicName)) =>
            data.remote.offer(ForwardPublish(publish, None))
            clientConnected(data)
          case (context, prl @ PublishReceivedLocally(publish, publishData))
              if data.publishers.exists(matchTopicFilter(_, publish.topicName)) =>
            val producerName = ActorName.mkName(ProducerNamePrefix + publish.topicName)
            context.child(producerName) match {
              case None if !data.pendingLocalPublications.exists(_._1 == publish.topicName) =>
                context.watchWith(
                  context.spawn(Producer(publish, publishData, selfAsRemote, data.producerPacketRouter, data.settings),
                                producerName),
                  ProducerFree(publish.topicName)
                )
                clientConnected(data)
              case _ =>
                clientConnected(
                  data.copy(pendingLocalPublications = data.pendingLocalPublications :+ (publish.topicName -> prl))
                )
            }
          case (context, ProducerFree(topicName)) =>
            val i = data.pendingLocalPublications.indexWhere(_._1 == topicName)
            if (i >= 0) {
              val prl = data.pendingLocalPublications(i)._2
              val producerName = ActorName.mkName(ProducerNamePrefix + topicName)
              context.watchWith(
                context.spawn(
                  Producer(prl.publish, prl.publishData, selfAsRemote, data.producerPacketRouter, data.settings),
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
              clientConnected(data)
            }
          case (context, ReceivedProducerPublishingCommand(command)) =>
            implicit val mat: Materializer = ActorMaterializer()(context.system)
            command.runWith(Sink.foreach {
              case Producer.ForwardPublish(publish, packetId) => data.remote.offer(ForwardPublish(publish, packetId))
              case Producer.ForwardPubRel(_, packetId) => data.remote.offer(ForwardPubRel(packetId))
            })
            Behaviors.same
          case (_, PingReqReceivedFromRemote(local)) =>
            data.remote.offer(ForwardPingResp)
            local ! ForwardPingReq
            clientConnected(data)
          case (_, ReceivePingReqTimeout) =>
            data.remote.fail(PingFailed)
            Behaviors.stopped // FIXME: It shouldn't stop as clients should be able to reconnect with the same client id
        } // TODO: what to do about idle connections...
        .receiveSignal {
          case (_, _: Terminated) =>
            Behaviors.same
          case (_, PostStop) =>
            data.remote.complete()
            Behaviors.same
        }
    }
  }

  def pendingSubAck(data: ConnAckReplied): Behavior[Event] =
    Behaviors
      .receivePartial[Event] {
        case (context, PublisherFree(topicFilters)) =>
          data.stash.foreach(context.self.tell)
          clientConnected(data.copy(publishers = data.publishers ++ topicFilters.map(_._1), stash = Vector.empty))
        case (_, e) if data.stash.size < data.settings.maxClientConnectionStashSize =>
          pendingSubAck(data.copy(stash = data.stash :+ e))
      }
      .receiveSignal {
        case (context, t: Terminated) if t.failure.contains(Publisher.SubscribeFailed) =>
          data.stash.foreach(context.self.tell)
          clientConnected(data)
        case (_, _: Terminated) =>
          Behaviors.same
        case (_, PostStop) =>
          data.remote.complete()
          Behaviors.same
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
  case object SubscribeFailed extends Exception

  /*
   * Construct with the starting state
   */
  def apply(packetId: PacketId,
            local: ActorRef[ForwardSubscribe.type],
            packetRouter: ActorRef[RemotePacketRouter.Request[Event]],
            settings: MqttSessionSettings): Behavior[Event] =
    preparePublisher(Start(packetId, local, packetRouter, settings))

  // Our FSM data, FSM events and commands emitted by the FSM

  sealed abstract class Data(val packetId: PacketId, val settings: MqttSessionSettings)
  final case class Start(override val packetId: PacketId,
                         local: ActorRef[ForwardSubscribe.type],
                         packetRouter: ActorRef[RemotePacketRouter.Request[Event]],
                         override val settings: MqttSessionSettings)
      extends Data(packetId, settings)
  final case class ServerSubscribe(override val packetId: PacketId,
                                   packetRouter: ActorRef[RemotePacketRouter.Request[Event]],
                                   override val settings: MqttSessionSettings)
      extends Data(packetId, settings)

  sealed abstract class Event
  final case object RegisteredPacketId extends Event
  final case object UnobtainablePacketId extends Event
  final case class SubAckReceivedLocally(remote: ActorRef[ForwardSubAck.type]) extends Event
  case object ReceiveSubAckTimeout extends Event

  sealed abstract class Command
  case object ForwardSubscribe extends Command
  case object ForwardSubAck extends Command

  // State event handling

  def preparePublisher(data: Start): Behavior[Event] = Behaviors.setup { context =>
    implicit val actorMqttSessionTimeout: Timeout = data.settings.actorMqttSessionTimeout

    context.ask[RemotePacketRouter.Register[Event], RemotePacketRouter.Registered.type](data.packetRouter)(
      replyTo => RemotePacketRouter.Register(context.self, data.packetId, replyTo)
    ) {
      case Success(RemotePacketRouter.Registered) => RegisteredPacketId
      case Failure(_) => UnobtainablePacketId
    }

    Behaviors.receiveMessagePartial {
      case RegisteredPacketId =>
        data.local ! ForwardSubscribe
        serverSubscribe(ServerSubscribe(data.packetId, data.packetRouter, data.settings))
      case UnobtainablePacketId =>
        Behaviors.stopped
    }
  }

  def serverSubscribe(data: ServerSubscribe): Behavior[Event] = Behaviors.withTimers { timer =>
    timer.startSingleTimer("receive-suback", ReceiveSubAckTimeout, data.settings.receiveSubAckTimeout)

    Behaviors
      .receiveMessagePartial[Event] {
        case SubAckReceivedLocally(remote) =>
          remote ! ForwardSubAck
          Behaviors.stopped
        case ReceiveSubAckTimeout =>
          throw SubscribeFailed
      }
      .receiveSignal {
        case (_, PostStop) =>
          data.packetRouter ! RemotePacketRouter.Unregister(data.packetId)
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
  case object UnsubscribeFailed extends Exception

  /*
   * Construct with the starting state
   */
  def apply(packetId: PacketId,
            local: ActorRef[ForwardUnsubscribe.type],
            packetRouter: ActorRef[RemotePacketRouter.Request[Event]],
            settings: MqttSessionSettings): Behavior[Event] =
    prepareServerUnpublisher(Start(packetId, local, packetRouter, settings))

  // Our FSM data, FSM events and commands emitted by the FSM

  sealed abstract class Data(val settings: MqttSessionSettings)
  final case class Start(packetId: PacketId,
                         local: ActorRef[ForwardUnsubscribe.type],
                         packetRouter: ActorRef[RemotePacketRouter.Request[Event]],
                         override val settings: MqttSessionSettings)
      extends Data(settings)
  final case class ServerUnsubscribe(packetId: PacketId,
                                     packetRouter: ActorRef[RemotePacketRouter.Request[Event]],
                                     override val settings: MqttSessionSettings)
      extends Data(settings)

  sealed abstract class Event
  final case object RegisteredPacketId extends Event
  final case object UnobtainablePacketId extends Event
  final case class UnsubAckReceivedLocally(remote: ActorRef[ForwardUnsubAck.type]) extends Event
  case object ReceiveUnsubAckTimeout extends Event

  sealed abstract class Command
  case object ForwardUnsubscribe extends Command
  case object ForwardUnsubAck extends Command

  // State event handling

  def prepareServerUnpublisher(data: Start): Behavior[Event] = Behaviors.setup { context =>
    implicit val actorMqttSessionTimeout: Timeout = data.settings.actorMqttSessionTimeout

    context.ask[RemotePacketRouter.Register[Event], RemotePacketRouter.Registered.type](data.packetRouter)(
      replyTo => RemotePacketRouter.Register(context.self, data.packetId, replyTo)
    ) {
      case Success(RemotePacketRouter.Registered) => RegisteredPacketId
      case Failure(_) => UnobtainablePacketId
    }

    Behaviors.receiveMessagePartial {
      case RegisteredPacketId =>
        data.local ! ForwardUnsubscribe
        serverUnsubscribe(ServerUnsubscribe(data.packetId, data.packetRouter, data.settings))
      case UnobtainablePacketId =>
        Behaviors.stopped
    }
  }

  def serverUnsubscribe(data: ServerUnsubscribe): Behavior[Event] = Behaviors.withTimers { timer =>
    timer.startSingleTimer("receive-unsubAck", ReceiveUnsubAckTimeout, data.settings.receiveUnsubAckTimeout)

    Behaviors
      .receiveMessagePartial[Event] {
        case UnsubAckReceivedLocally(remote) =>
          remote ! ForwardUnsubAck
          Behaviors.stopped
        case ReceiveUnsubAckTimeout =>
          throw UnsubscribeFailed
      }
      .receiveSignal {
        case (_, PostStop) =>
          data.packetRouter ! RemotePacketRouter.Unregister(data.packetId)
          Behaviors.same
      }
  }
}
