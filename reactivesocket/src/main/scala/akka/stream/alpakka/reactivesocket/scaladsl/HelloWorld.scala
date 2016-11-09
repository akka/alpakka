package akka.stream.alpakka.reactivesocket.scaladsl

import io.reactivesocket.transport.tcp.server.TcpTransportServer
import io.reactivesocket.server.ReactiveSocketServer
import io.reactivesocket.server.ReactiveSocketServer.SocketAcceptor
import io.reactivesocket.ConnectionSetupPayload
import io.reactivesocket.lease.LeaseEnforcingSocket
import io.reactivesocket.ReactiveSocket
import io.reactivesocket.lease.DisabledLeaseAcceptingSocket
import io.reactivesocket.AbstractReactiveSocket
import org.reactivestreams.Publisher
import io.reactivesocket.Payload
import io.reactivesocket.client.ReactiveSocketClient
import io.reactivesocket.transport.tcp.client.TcpTransportClient

import io.reactivesocket.client.KeepAliveProvider._
import io.reactivesocket.client.SetupProvider._
import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Source
import akka.stream.scaladsl.Sink
import io.reactivesocket.util.PayloadImpl
import scala.concurrent.Future
import io.reactivesocket.frame.ByteBufferUtil

object HelloWorld {
  def main(args: Array[String]): Unit =
    (new HelloWorld).start()
}

class HelloWorld {
  val system = ActorSystem("HelloWorld")
  implicit val materializer = ActorMaterializer.create(system)
  import system.dispatcher

  def start(): Unit = {
    val server = ReactiveSocketServer.create(TcpTransportServer.create())
      .start(socketAcceptor)

    val serverAddress = server.getServerAddress
    println(s"started server: $serverAddress") // FIXME

    val clientPublisher = ReactiveSocketClient.create(TcpTransportClient.create(serverAddress),
      keepAlive(never()).disableLease()).connect()

    Source.fromPublisher(clientPublisher).runWith(Sink.head).foreach { socket =>
      println(s"# got socket") // FIXME
      val response: Future[Payload] =
        Source.fromPublisher(socket.requestResponse(new PayloadImpl("Hello"))).runWith(Sink.head)

      response.onFailure {
        case e => println(e.getMessage)
      }

      response.map(_.getData)
        .map(x => { println("got response"); x }) // FIXME remove
        .map(ByteBufferUtil.toUtf8String)
        .map(println)
        .andThen { case _ => socket.close() }
    }

  }

  def socketAcceptor: SocketAcceptor = new SocketAcceptor {
    override def accept(setup: ConnectionSetupPayload, sendingSocket: ReactiveSocket): LeaseEnforcingSocket =
      new DisabledLeaseAcceptingSocket(new AbstractReactiveSocket {
        override def requestResponse(p: Payload): Publisher[Payload] = {
          println(s"# got request") // FIXME
          Source.single(p).runWith(Sink.asPublisher(fanout = false))
        }
      })
  }
}
