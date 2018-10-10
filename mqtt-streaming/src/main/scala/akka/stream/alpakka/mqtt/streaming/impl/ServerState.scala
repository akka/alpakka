/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.mqtt.streaming
package impl

import java.nio.charset.StandardCharsets

import akka.NotUsed
import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.{ActorRef, Behavior, PostStop}
import akka.annotation.InternalApi
import akka.stream.{Materializer, OverflowStrategy}
import akka.stream.scaladsl.{BroadcastHub, Keep, Source, SourceQueueWithComplete}
import akka.stream.typed.scaladsl.ActorMaterializer
import akka.util.ByteString

import scala.annotation.tailrec

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
            settings: MqttSessionSettings): Behavior[Event] =
    listening(Listening(Map.empty, consumerPacketRouter, producerPacketRouter, settings))

  // Our FSM data, FSM events and commands emitted by the FSM

  sealed abstract class Data(val consumerPacketRouter: ActorRef[RemotePacketRouter.Request[Consumer.Event]],
                             val producerPacketRouter: ActorRef[LocalPacketRouter.Request[Producer.Event]],
                             val settings: MqttSessionSettings)
  final case class Listening(clientConnections: Map[ByteString, ActorRef[ClientConnection.Event]],
                             override val consumerPacketRouter: ActorRef[RemotePacketRouter.Request[Consumer.Event]],
                             override val producerPacketRouter: ActorRef[LocalPacketRouter.Request[Producer.Event]],
                             override val settings: MqttSessionSettings)
      extends Data(consumerPacketRouter, producerPacketRouter, settings)
  final case class Accepting(stash: Seq[Event],
                             override val consumerPacketRouter: ActorRef[RemotePacketRouter.Request[Consumer.Event]],
                             override val producerPacketRouter: ActorRef[LocalPacketRouter.Request[Producer.Event]],
                             override val settings: MqttSessionSettings)
      extends Data(consumerPacketRouter, producerPacketRouter, settings)

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
                                               local: ActorRef[ClientConnection.ForwardConnAckCommand])
      extends Event(connectionId)
  final case class DisconnectReceivedFromRemote(override val connectionId: ByteString,
                                                local: ActorRef[ClientConnection.ForwardDisconnect.type])
      extends Event(connectionId)
  final case class ConnectionLost(override val connectionId: ByteString) extends Event(connectionId)
  final case class ClientConnectionFree(override val connectionId: ByteString) extends Event(connectionId)

  // State event handling

  private val ClientConnectionNamePrefix = "client-connection-"

  private def mkActorName(name: String): String =
    name.getBytes(StandardCharsets.UTF_8).map(_.toHexString).mkString

  private def forward(connectionId: ByteString,
                      clientConnections: Map[ByteString, ActorRef[ClientConnection.Event]],
                      e: ClientConnection.Event): Behavior[Event] = {
    clientConnections.get(connectionId).foreach(_ ! e)
    Behaviors.same
  }

  def listening(data: Listening): Behavior[Event] = Behaviors.receivePartial {
    case (context, ConnectReceivedFromRemote(connectionId, connect, local)) =>
      val clientConnectionName = mkActorName(ClientConnectionNamePrefix + connectionId)
      context.child(clientConnectionName) match {
        case None =>
          val clientConnection = context.spawn(
            ClientConnection(connect, local, data.consumerPacketRouter, data.producerPacketRouter, data.settings),
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
  }
}

/*
 * Handles events in relation to a specific client connection
 */
@InternalApi private[streaming] object ClientConnection {

  /*
   * Construct with the starting state
   */
  def apply(connect: Connect,
            local: ActorRef[ForwardConnect.type],
            consumerPacketRouter: ActorRef[RemotePacketRouter.Request[Consumer.Event]],
            producerPacketRouter: ActorRef[LocalPacketRouter.Request[Producer.Event]],
            settings: MqttSessionSettings): Behavior[Event] =
    clientConnect(ConnectReceived(local, Vector.empty, consumerPacketRouter, producerPacketRouter, settings))

  // Our FSM data, FSM events and commands emitted by the FSM

  sealed abstract class Data(val consumerPacketRouter: ActorRef[RemotePacketRouter.Request[Consumer.Event]],
                             val producerPacketRouter: ActorRef[LocalPacketRouter.Request[Producer.Event]],
                             val settings: MqttSessionSettings)
  final case class ConnectReceived(
      local: ActorRef[ForwardConnect.type],
      stash: Seq[Event],
      override val consumerPacketRouter: ActorRef[RemotePacketRouter.Request[Consumer.Event]],
      override val producerPacketRouter: ActorRef[LocalPacketRouter.Request[Producer.Event]],
      override val settings: MqttSessionSettings
  ) extends Data(consumerPacketRouter, producerPacketRouter, settings)
  final case class ConnAckReplied(
      remote: SourceQueueWithComplete[ForwardConnAckCommand],
      subscriptions: Map[String, ActorRef[Publisher.Event]],
      override val consumerPacketRouter: ActorRef[RemotePacketRouter.Request[Consumer.Event]],
      override val producerPacketRouter: ActorRef[LocalPacketRouter.Request[Producer.Event]],
      override val settings: MqttSessionSettings
  ) extends Data(consumerPacketRouter, producerPacketRouter, settings)

  sealed abstract class Event
  case object ReceiveConnAckTimeout extends Event
  final case class ConnAckReceivedLocally(connAck: ConnAck, remote: ActorRef[Source[ForwardConnAckCommand, NotUsed]])
      extends Event
  final case class SubscribeReceivedFromRemote(subscribe: Subscribe, local: ActorRef[ForwardConnAckCommand])
      extends Event
  final case class DisconnectReceivedFromRemote(local: ActorRef[ForwardDisconnect.type]) extends Event
  case object ConnectionLost extends Event

  sealed abstract class Command
  case object ForwardConnect extends Command
  sealed abstract class ForwardConnAckCommand
  case object ForwardConnAck extends ForwardConnAckCommand
  case object ForwardDisconnect extends Command

  // State event handling

  def clientConnect(data: ConnectReceived): Behavior[Event] = Behaviors.setup { _ =>
    data.local ! ForwardConnect

    Behaviors.withTimers { timer =>
      timer.startSingleTimer("receive-connack", ReceiveConnAckTimeout, data.settings.receiveConnAckTimeout)

      Behaviors
        .receivePartial[Event] {
          case (context, ConnAckReceivedLocally(_, remote)) =>
            implicit val mat: Materializer = ActorMaterializer()(context.system)
            val (queue, source) = Source
              .queue[ForwardConnAckCommand](1, OverflowStrategy.dropHead)
              .toMat(BroadcastHub.sink)(Keep.both)
              .run()
            remote ! source

            queue.offer(ForwardConnAck)
            data.stash.foreach(context.self.tell)

            clientConnected(
              ConnAckReplied(queue, Map.empty, data.consumerPacketRouter, data.producerPacketRouter, data.settings)
            )
          case (_, ReceiveConnAckTimeout) =>
            Behaviors.stopped
          case (_, e) if data.stash.size < data.settings.maxConnectStashSize =>
            clientConnect(data.copy(stash = data.stash :+ e))
        }
        .receiveSignal {
          case (_, PostStop) =>
            Behaviors.same
        }

    }
  }

  def clientConnected(data: ConnAckReplied): Behavior[Event] =
    Behaviors
      .receiveMessagePartial[Event] {
        case DisconnectReceivedFromRemote(local) =>
          local ! ForwardDisconnect
          Behaviors.stopped
        case SubscribeReceivedFromRemote(subscribe, local) =>
          Behaviors.same // FIXME
      }
      .receiveSignal {
        case (_, PostStop) =>
          data.remote.complete()
          Behaviors.same
      }
}

/*
 * A publisher receives client subscriptions
 * and forwards local publications through them
 */
@InternalApi private[streaming] object Publisher {

  /*
   * Construct with the starting state
   */
  def apply(local: ActorRef[ForwardSubscribe.type], settings: MqttSessionSettings): Behavior[Event] =
    clientSubscribed(Start(local, settings))

  // Our FSM data, FSM events and commands emitted by the FSM

  sealed abstract class Data(val settings: MqttSessionSettings)
  final case class Start(local: ActorRef[ForwardSubscribe.type], override val settings: MqttSessionSettings)
      extends Data(settings)

  sealed abstract class Event
  final case class SubAckReceivedLocally(remote: ActorRef[Source[ForwardSubAckCommand, NotUsed]]) extends Event
  case object ReceiveSubAckTimeout extends Event

  sealed abstract class Command
  case object ForwardSubscribe extends Command
  sealed abstract class ForwardSubAckCommand
  case object ForwardConnAck extends ForwardSubAckCommand

  // State event handling

  def clientSubscribed(start: Start): Behavior[Event] = ???

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
