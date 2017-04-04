package akka.stream.alpakka.udp.sink

import java.net.InetSocketAddress

import akka.actor.{ActorRef, ActorSystem}
import akka.io.{IO, Udp}
import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler}
import akka.stream.{Attributes, Inlet, SinkShape}
import akka.util.ByteString

final case class UdpMessage(data: ByteString, remote: InetSocketAddress)

class UdpSinkStageLogic(val shape: SinkShape[UdpMessage])(implicit val system: ActorSystem) extends GraphStageLogic(shape) {
  implicit def self: ActorRef = stageActor.ref
  private def in = shape.in
  private var udpSender: Option[ActorRef] = None
  override def preStart(): Unit = {
    getStageActor(processIncoming)
    IO(Udp) ! Udp.SimpleSender
  }
  private def processIncoming(event: (ActorRef, Any)): Unit = {
    val s: ActorRef = event._1
    val msg = event._2
    msg match {
      case ready: Udp.SimpleSenderReady ⇒
        udpSender = Some(s)
        setKeepGoing(true);
        pull(in);
    }
  }
  setHandler(in, new InHandler {
    override def onPush() = {
      grab(in) match {
        case impl: UdpMessage ⇒
          udpSender.foreach { sender ⇒
            sender ! Udp.Send(impl.data, impl.remote)
          }
        case _ ⇒
      }
      pull(in)
    }
  })
}

class UdpSink(implicit val system: ActorSystem) extends GraphStage[SinkShape[UdpMessage]] {
  val in: Inlet[UdpMessage] = Inlet("UdpSink.in")
  val shape: SinkShape[UdpMessage] = SinkShape.of(in)
  override def createLogic(inheritedAttributes: Attributes) = {
    new UdpSinkStageLogic(shape)
  }
}
