/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.mqtt.streaming
package impl

import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.Behaviors
import akka.annotation.InternalApi

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
  final case class ConnectReceived(connect: Connect, override val settings: MqttSessionSettings) extends Data(settings)
  final case class ConnAckReceived(connect: Connect, connAck: ConnAck, override val settings: MqttSessionSettings)
      extends Data(settings)

  sealed trait Event
  final case class ConnectReceivedLocally(connect: Connect) extends Event
  final case class ConnAckReceivedFromRemote(connAck: ConnAck) extends Event
  case object ReceiveConnAckTimeout extends Event
  case object LostConnection extends Event
  case object DisconnectReceivedLocally extends Event

  // State event handling

  def disconnected(data: Uninitialized): Behavior[Event] = Behaviors.receiveMessagePartial {
    case ConnectReceivedLocally(connect) =>
      serverConnect(ConnectReceived(connect, data.settings))
  }

  def serverConnect(data: ConnectReceived): Behavior[Event] = Behaviors.withTimers { timer =>
    forwardConnectToRemote(data)
    timer.startSingleTimer("receive-connack", ReceiveConnAckTimeout, data.settings.receiveConnAckTimeout)
    Behaviors.receiveMessagePartial {
      case ConnAckReceivedFromRemote(connAck) if connAck.returnCode.contains(ConnAckReturnCode.ConnectionAccepted) =>
        serverConnected(ConnAckReceived(data.connect, connAck, data.settings))
      case _: ConnAckReceivedFromRemote =>
        disconnected(Uninitialized(data.settings))
      case ReceiveConnAckTimeout =>
        disconnected(Uninitialized(data.settings))
    }
  }

  def serverConnected(data: ConnAckReceived): Behavior[Event] = {
    forwardConnAckToLocal(data)
    if (data.connect.connectFlags.contains(ConnectFlags.CleanSession)) cleanSession()
    Behaviors.receiveMessagePartial {
      case LostConnection =>
        disconnected(Uninitialized(data.settings))
      case DisconnectReceivedLocally =>
        forwardDisconnectToRemote()
        cleanSession()
        disconnected(Uninitialized(data.settings))
      // TODO: UnsubscribedReceivedLocally, SubscribeReceivedLocally, PublishReceivedFromRemote, PubRelReceivedFromRemote, PubAckReceivedLocally, PubRecReceivedLocally, PubCompReceivedLocally, PublishReceivedLocally, PubRecReceivedLocally, PubCompReceivedLocally, PubAckReceivedFromRemote, PubRelReceivedFromRemote
    }
  }

  // Actions

  def forwardConnectToRemote(data: ConnectReceived): Unit = ???

  def forwardConnAckToLocal(data: ConnAckReceived): Unit = ???

  def forwardDisconnectToRemote(): Unit = ???

  def cleanSession(): Unit = ???
}
