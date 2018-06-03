/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.udp.impl

import akka.actor.{ActorRef, ActorSystem, PoisonPill}
import akka.io.{IO, Udp}
import akka.stream.alpakka.udp.UdpMessage
import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, OutHandler}
import akka.stream._

class UdpFireAndForgetLogic(val shape: FlowShape[UdpMessage, UdpMessage])(implicit val system: ActorSystem)
    extends GraphStageLogic(shape) {

  implicit def self: ActorRef = stageActor.ref

  private def in = shape.in
  private def out = shape.out

  private var simpleSender: ActorRef = _

  override def preStart(): Unit = {
    getStageActor(processIncoming)
    IO(Udp) ! Udp.SimpleSender
  }

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

      override def onUpstreamFinish(): Unit = {
        stopSimpleSender()
        super.onUpstreamFinish()
      }

      override def onUpstreamFailure(ex: Throwable): Unit = {
        stopSimpleSender()
        super.onUpstreamFailure(ex)
      }
    }
  )

  setHandler(
    out,
    new OutHandler {
      override def onPull(): Unit = if (simpleSender != null) pull(in)

      override def onDownstreamFinish(): Unit = {
        stopSimpleSender()
        super.onDownstreamFinish()
      }
    }
  )
}

class UdpFireAndForgetFlow(implicit val system: ActorSystem) extends GraphStage[FlowShape[UdpMessage, UdpMessage]] {

  val in: Inlet[UdpMessage] = Inlet("UdpFireAndForgetFlow.in")
  val out: Outlet[UdpMessage] = Outlet("UdpFireAndForgetFlow.in")

  val shape: FlowShape[UdpMessage, UdpMessage] = FlowShape.of(in, out)
  override def createLogic(inheritedAttributes: Attributes) = new UdpFireAndForgetLogic(shape)
}
