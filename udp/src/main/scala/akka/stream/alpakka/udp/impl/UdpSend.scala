/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.udp.impl

import akka.actor.{ActorRef, ActorSystem, PoisonPill}
import akka.annotation.InternalApi
import akka.io.{IO, Udp}
import akka.stream.alpakka.udp.UdpMessage
import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, OutHandler}
import akka.stream._

/**
 * Sends incoming messages to the corresponding destination addresses.
 * After send command is issued to the UDP manager actor the message
 * is passed-through to the output for possible further processing.
 */
@InternalApi
final class UdpSendLogic(val shape: FlowShape[UdpMessage, UdpMessage])(implicit val system: ActorSystem)
    extends GraphStageLogic(shape) {

  implicit def self: ActorRef = stageActor.ref

  private def in = shape.in
  private def out = shape.out

  private var simpleSender: ActorRef = _

  override def preStart(): Unit = {
    getStageActor(processIncoming)
    IO(Udp) ! Udp.SimpleSender
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

@InternalApi
final class UdpSendFlow(implicit val system: ActorSystem) extends GraphStage[FlowShape[UdpMessage, UdpMessage]] {

  val in: Inlet[UdpMessage] = Inlet("UdpSendFlow.in")
  val out: Outlet[UdpMessage] = Outlet("UdpSendFlow.in")

  val shape: FlowShape[UdpMessage, UdpMessage] = FlowShape.of(in, out)
  override def createLogic(inheritedAttributes: Attributes) = new UdpSendLogic(shape)
}
