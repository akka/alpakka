/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.mqtt.streaming
package impl

import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.Behaviors
import akka.annotation.InternalApi

import scala.concurrent.duration._

@InternalApi private[streaming] object MqttClient {

  private val receiveConnAckTimeout = 3.seconds // FIXME: Should be a setting

  sealed trait Data
  case object Unitialized extends Data
  final case class ConnectReceived(connect: Connect) extends Data
  final case class ConnAckReceived(connect: Connect, connAck: ConnAck) extends Data

  sealed trait Event
  final case class ConnectReceivedLocally(connect: Connect) extends Event
  final case class ConnAckReceivedFromRemote(connAck: ConnAck) extends Event
  case object ReceiveConnAckTimeout extends Event
  case object LostConnection extends Event
  case object DisconnectReceivedLocally extends Event

  // State handling

  def Disconnected(data: Unitialized.type): Behavior[Event] = Behaviors.receiveMessage {
    case ConnectReceivedLocally(connect) =>
      ServerConnect(ConnectReceived(connect))
    case _ =>
      Behaviors.unhandled
  }

  def ServerConnect(data: ConnectReceived): Behavior[Event] = Behaviors.withTimers { timer =>
    forwardConnectToRemote(data)
    timer.startSingleTimer("receive-connack", ReceiveConnAckTimeout, receiveConnAckTimeout)
    Behaviors.receiveMessagePartial {
      case ConnAckReceivedFromRemote(connAck) if connAck.returnCode.contains(ConnAckReturnCode.ConnectionAccepted) =>
        ServerConnected(ConnAckReceived(data.connect, connAck))
      case _: ConnAckReceivedFromRemote =>
        Disconnected(Unitialized)
      case ReceiveConnAckTimeout =>
        Disconnected(Unitialized)
      case _ =>
        Behaviors.unhandled
    }
  }

  def ServerConnected(data: ConnAckReceived): Behavior[Event] = {
    forwardConnAckToLocal(data)
    if (data.connect.connectFlags.contains(ConnectFlags.CleanSession)) cleanSession()
    Behaviors.receiveMessagePartial {
      case LostConnection =>
        Disconnected(Unitialized)
      case DisconnectReceivedLocally =>
        forwardDisconnectToRemote()
        cleanSession()
        Disconnected(Unitialized)
      // TODO: UnsubscribedReceivedLocally, SubscribeReceivedLocally, PublishReceivedFromRemote, PubRelReceivedFromRemote, PubAckReceivedLocally, PubRecReceivedLocally, PubCompReceivedLocally, PublishReceivedLocally, PubRecReceivedLocally, PubCompReceivedLocally, PubAckReceivedFromRemote, PubRelReceivedFromRemote
      case _ =>
        Behaviors.unhandled
    }
  }

  // Actions

  def forwardConnectToRemote(data: ConnectReceived): Unit = ???

  def forwardConnAckToLocal(data: ConnAckReceived): Unit = ???

  def forwardDisconnectToRemote(): Unit = ???

  def cleanSession(): Unit = ???
}
