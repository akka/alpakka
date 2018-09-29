/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.mqtt.streaming
package impl

import akka.actor.typed.{ActorRef, Behavior}
import akka.actor.typed.scaladsl.Behaviors
import akka.annotation.InternalApi
import akka.util.ByteString

/*
 * A client connector is a Finite State Machine that manages MQTT client
 * session state. A client connects to a server, subscribes/unsubscribes
 * from topics to receive publications on and publishes to its own topics.
 */
@InternalApi private[streaming] object ClientConnector {

  /*
   * Construct with the starting state
   */
  def apply(settings: MqttSessionSettings): Behavior[Event] =
    disconnected(Uninitialized(settings))

  // Our FSM data and events

  sealed abstract class Data(val settings: MqttSessionSettings)
  final case class Uninitialized(override val settings: MqttSessionSettings) extends Data(settings)
  final case class ConnectReceived(connect: Connect, connectData: Option[_], override val settings: MqttSessionSettings)
      extends Data(settings)
  final case class ConnAckReceived(connect: Connect, connAck: ConnAck, override val settings: MqttSessionSettings)
      extends Data(settings)

  sealed abstract class Event
  final case class ConnectReceivedLocally(connect: Connect, connectData: Option[_], remote: ActorRef[ByteString])
      extends Event
  final case class ConnAckReceivedFromRemote(connAck: ConnAck, local: ActorRef[(ConnAck, Option[_])]) extends Event
  case object ReceiveConnAckTimeout extends Event
  case object LostConnection extends Event
  case object DisconnectReceivedLocally extends Event

  // State event handling

  def disconnected(data: Uninitialized): Behavior[Event] = Behaviors.receiveMessagePartial {
    case ConnectReceivedLocally(connect, connectData, remote) =>
      forwardConnectToRemote(connect, remote)
      serverConnect(ConnectReceived(connect, connectData, data.settings))
  }

  def serverConnect(data: ConnectReceived): Behavior[Event] = Behaviors.withTimers { timer =>
    timer.startSingleTimer("receive-connack", ReceiveConnAckTimeout, data.settings.receiveConnAckTimeout)
    Behaviors.receiveMessagePartial {
      case ConnAckReceivedFromRemote(connAck, local)
          if connAck.returnCode.contains(ConnAckReturnCode.ConnectionAccepted) =>
        forwardConnAckToLocal(connAck, data.connectData, local)
        if (data.connect.connectFlags.contains(ConnectFlags.CleanSession)) cleanSession()
        serverConnected(ConnAckReceived(data.connect, connAck, data.settings))
      case ConnAckReceivedFromRemote(connAck, local) =>
        forwardConnAckToLocal(connAck, data.connectData, local)
        disconnected(Uninitialized(data.settings))
      case ReceiveConnAckTimeout =>
        disconnected(Uninitialized(data.settings))
    }
  }

  def serverConnected(data: ConnAckReceived): Behavior[Event] = Behaviors.receiveMessagePartial {
    case LostConnection =>
      disconnected(Uninitialized(data.settings))
    case DisconnectReceivedLocally =>
      forwardDisconnectToRemote()
      cleanSession()
      disconnected(Uninitialized(data.settings))
    // TODO: UnsubscribedReceivedLocally, SubscribeReceivedLocally, PublishReceivedFromRemote, PubRelReceivedFromRemote, PubAckReceivedLocally, PubRecReceivedLocally, PubCompReceivedLocally, PublishReceivedLocally, PubRecReceivedLocally, PubCompReceivedLocally, PubAckReceivedFromRemote, PubRelReceivedFromRemote
  }

  // Actions

  import MqttCodec._

  def forwardConnectToRemote(connect: Connect, remote: ActorRef[ByteString]): Unit =
    remote ! connect.encode(ByteString.newBuilder).result()

  def forwardConnAckToLocal(connAck: ConnAck, connectData: Option[_], local: ActorRef[(ConnAck, Option[_])]): Unit =
    local ! ((connAck, connectData))

  def forwardDisconnectToRemote(): Unit = ???

  def cleanSession(): Unit = ???
}
