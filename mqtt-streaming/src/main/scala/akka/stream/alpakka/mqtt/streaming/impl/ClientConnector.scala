/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.mqtt.streaming
package impl

import akka.actor.typed.{ActorRef, Behavior}
import akka.actor.typed.scaladsl.Behaviors
import akka.annotation.InternalApi

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
  def apply(settings: MqttSessionSettings): Behavior[Event] =
    disconnected(Uninitialized(settings))

  // Our FSM data, FSM events and commands emitted by the FSM

  sealed abstract class Data(val settings: MqttSessionSettings)
  final case class Uninitialized(override val settings: MqttSessionSettings) extends Data(settings)
  final case class ConnectReceived(connect: Connect,
                                   connectData: ConnectData,
                                   override val settings: MqttSessionSettings)
      extends Data(settings)
  final case class ConnAckReceived(connect: Connect, connAck: ConnAck, override val settings: MqttSessionSettings)
      extends Data(settings)

  sealed abstract class Event
  final case class ConnectReceivedLocally(connect: Connect,
                                          connectData: ConnectData,
                                          remote: ActorRef[ForwardConnect.type])
      extends Event
  final case class ConnAckReceivedFromRemote(connAck: ConnAck, local: ActorRef[ForwardConnAck]) extends Event
  case object ReceiveConnAckTimeout extends Event
  case object ConnectionLost extends Event
  final case class DisconnectReceivedLocally(remote: ActorRef[ForwardDisconnect.type]) extends Event

  sealed abstract class Command
  case object ForwardConnect extends Command
  final case class ForwardConnAck(connectData: ConnectData) extends Command
  case object ForwardDisconnect extends Command

  // State event handling

  def disconnected(data: Uninitialized): Behavior[Event] = Behaviors.receiveMessagePartial {
    case ConnectReceivedLocally(connect, connectData, remote) =>
      remote ! ForwardConnect
      serverConnect(ConnectReceived(connect, connectData, data.settings))
  }

  def serverConnect(data: ConnectReceived): Behavior[Event] = Behaviors.withTimers { timer =>
    timer.startSingleTimer("receive-connack", ReceiveConnAckTimeout, data.settings.receiveConnAckTimeout)
    Behaviors.receiveMessagePartial {
      case ConnAckReceivedFromRemote(connAck, local)
          if connAck.returnCode.contains(ConnAckReturnCode.ConnectionAccepted) =>
        local ! ForwardConnAck(data.connectData)
        if (data.connect.connectFlags.contains(ConnectFlags.CleanSession)) cleanSession()
        serverConnected(ConnAckReceived(data.connect, connAck, data.settings))
      case ConnAckReceivedFromRemote(_, local) =>
        local ! ForwardConnAck(data.connectData)
        disconnected(Uninitialized(data.settings))
      case ReceiveConnAckTimeout =>
        disconnected(Uninitialized(data.settings))
    }
  }

  def serverConnected(data: ConnAckReceived): Behavior[Event] = Behaviors.receiveMessagePartial {
    case ConnectionLost =>
      disconnected(Uninitialized(data.settings))
    case DisconnectReceivedLocally(remote) =>
      remote ! ForwardDisconnect
      cleanSession()
      disconnected(Uninitialized(data.settings))
    // TODO: UnsubscribedReceivedLocally, SubscribeReceivedLocally, PublishReceivedFromRemote, PubRelReceivedFromRemote, PubAckReceivedLocally, PubRecReceivedLocally, PubCompReceivedLocally, PublishReceivedLocally, PubRecReceivedLocally, PubCompReceivedLocally, PubAckReceivedFromRemote, PubRelReceivedFromRemote
  }

  // Actions

  def cleanSession(): Unit = {} // TODO
}
