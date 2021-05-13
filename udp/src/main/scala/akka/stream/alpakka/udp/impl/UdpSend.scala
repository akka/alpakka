/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.udp.impl

import akka.actor.{ActorRef, ActorSystem, PoisonPill}
import akka.annotation.InternalApi
import akka.io.{IO, Udp}
import akka.io.Inet.SocketOption
import akka.stream.alpakka.udp.Datagram
import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, OutHandler}
import akka.stream._

import scala.Option
import scala.collection.immutable.Iterable

/**
 * Sends incoming messages to the corresponding destination addresses.
 * After send command is issued to the UDP manager actor the message
 * is passed-through to the output for possible further processing.
 */
@InternalApi private[udp] final class UdpSendLogic(val shape: FlowShape[Datagram, Datagram],
                                                   options: Option[Iterable[SocketOption]])(
    implicit val system: ActorSystem
) extends GraphStageLogic(shape) {

  implicit def self: ActorRef = stageActor.ref

  private def in = shape.in
  private def out = shape.out

  private var simpleSender: ActorRef = _

  override def preStart(): Unit = {
    getStageActor(processIncoming)
    options match {
      case Some(socketOptions) => IO(Udp) ! Udp.SimpleSender(socketOptions)
      case None => IO(Udp) ! Udp.SimpleSender
    }
  }

  override def postStop(): Unit =
    stopSimpleSender()

  private def processIncoming(event: (ActorRef, Any)): Unit = event match {
    case (sender, Udp.SimpleSenderReady) ⇒
      simpleSender = sender
      pull(in)
    case _ =>
  }

  private def stopSimpleSender() =
    if (simpleSender != null) {
      simpleSender ! PoisonPill
    }

  setHandler(
    in,
    new InHandler {
      override def onPush() = {
        val msg = grab(in)
        simpleSender ! Udp.Send(msg.data, msg.remote)
        push(out, msg)
      }
    }
  )

  setHandler(
    out,
    new OutHandler {
      override def onPull(): Unit = if (simpleSender != null) pull(in)
    }
  )
}

@InternalApi private[udp] final class UdpSendFlow(options: Option[Iterable[SocketOption]])(
    implicit val system: ActorSystem
) extends GraphStage[FlowShape[Datagram, Datagram]] {

  val in: Inlet[Datagram] = Inlet("UdpSendFlow.in")
  val out: Outlet[Datagram] = Outlet("UdpSendFlow.in")

  val shape: FlowShape[Datagram, Datagram] = FlowShape.of(in, out)
  override def createLogic(inheritedAttributes: Attributes) = new UdpSendLogic(shape, options)
}
