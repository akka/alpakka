/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.mqtt.streaming
package impl

import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.Behaviors
import akka.annotation.InternalApi

import scala.concurrent.duration._

@InternalApi private[streaming] object ClientConnector {

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

  def disconnected(data: Unitialized.type): Behavior[Event] = Behaviors.receiveMessagePartial {
    case ConnectReceivedLocally(connect) =>
      serverConnect(ConnectReceived(connect))
  }

  def serverConnect(data: ConnectReceived): Behavior[Event] = Behaviors.withTimers { timer =>
    forwardConnectToRemote(data)
    timer.startSingleTimer("receive-connack", ReceiveConnAckTimeout, receiveConnAckTimeout)
    Behaviors.receiveMessagePartial {
      case ConnAckReceivedFromRemote(connAck) if connAck.returnCode.contains(ConnAckReturnCode.ConnectionAccepted) =>
        serverConnected(ConnAckReceived(data.connect, connAck))
      case _: ConnAckReceivedFromRemote =>
        disconnected(Unitialized)
      case ReceiveConnAckTimeout =>
        disconnected(Unitialized)
    }
  }

  def serverConnected(data: ConnAckReceived): Behavior[Event] = {
    forwardConnAckToLocal(data)
    if (data.connect.connectFlags.contains(ConnectFlags.CleanSession)) cleanSession()
    Behaviors.receiveMessagePartial {
      case LostConnection =>
        disconnected(Unitialized)
      case DisconnectReceivedLocally =>
        forwardDisconnectToRemote()
        cleanSession()
        disconnected(Unitialized)
      // TODO: UnsubscribedReceivedLocally, SubscribeReceivedLocally, PublishReceivedFromRemote, PubRelReceivedFromRemote, PubAckReceivedLocally, PubRecReceivedLocally, PubCompReceivedLocally, PublishReceivedLocally, PubRecReceivedLocally, PubCompReceivedLocally, PubAckReceivedFromRemote, PubRelReceivedFromRemote
    }
  }

  // Actions

  def forwardConnectToRemote(data: ConnectReceived): Unit = ???

  def forwardConnAckToLocal(data: ConnAckReceived): Unit = ???

  def forwardDisconnectToRemote(): Unit = ???

  def cleanSession(): Unit = ???
}
