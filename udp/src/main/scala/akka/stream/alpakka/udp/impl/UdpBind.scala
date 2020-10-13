/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.udp.impl

import java.net.InetSocketAddress

import akka.actor.{ActorRef, ActorSystem}
import akka.annotation.InternalApi
import akka.io.{IO, Udp}
import akka.stream.{Attributes, FlowShape, Inlet, Outlet}
import akka.stream.alpakka.udp.Datagram
import akka.stream.stage._

import scala.concurrent.{Future, Promise}

/**
 * Binds to the given local address using UDP manager actor.
 */
@InternalApi private[udp] final class UdpBindLogic(localAddress: InetSocketAddress,
                                                   boundPromise: Promise[InetSocketAddress]
)(
    val shape: FlowShape[Datagram, Datagram]
)(implicit val system: ActorSystem)
    extends GraphStageLogic(shape) {

  private def in = shape.in
  private def out = shape.out

  private var listener: ActorRef = _

  override def preStart(): Unit = {
    implicit val sender = getStageActor(processIncoming).ref
    IO(Udp) ! Udp.Bind(sender, localAddress)
  }

  override def postStop(): Unit =
    unbindListener()

  private def processIncoming(event: (ActorRef, Any)): Unit = event match {
    case (sender, Udp.Bound(boundAddress)) ⇒
      boundPromise.success(boundAddress)
      listener = sender
      pull(in)
    case (_, Udp.CommandFailed(cmd: Udp.Bind)) =>
      val ex = new IllegalArgumentException(s"Unable to bind to [${cmd.localAddress}]")
      boundPromise.failure(ex)
      failStage(ex)
    case (_, Udp.Received(data, sender)) =>
      if (isAvailable(out)) {
        push(out, Datagram(data, sender))
      }
    case _ =>
  }

  private def unbindListener() =
    if (listener != null) {
      listener ! Udp.Unbind
    }

  setHandler(
    in,
    new InHandler {
      override def onPush() = {
        val msg = grab(in)
        listener ! Udp.Send(msg.data, msg.remote)
        pull(in)
      }
    }
  )

  setHandler(
    out,
    new OutHandler {
      override def onPull(): Unit = ()
    }
  )
}

@InternalApi private[udp] final class UdpBindFlow(localAddress: InetSocketAddress)(implicit val system: ActorSystem)
    extends GraphStageWithMaterializedValue[FlowShape[Datagram, Datagram], Future[InetSocketAddress]] {

  val in: Inlet[Datagram] = Inlet("UdpBindFlow.in")
  val out: Outlet[Datagram] = Outlet("UdpBindFlow.in")

  val shape: FlowShape[Datagram, Datagram] = FlowShape.of(in, out)
  override def createLogicAndMaterializedValue(inheritedAttributes: Attributes) = {
    val boundPromise = Promise[InetSocketAddress]
    (new UdpBindLogic(localAddress, boundPromise)(shape), boundPromise.future)
  }
}
