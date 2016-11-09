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
import akka.stream.Materializer
import akka.Done
import akka.NotUsed

object HelloWorld {
  def main(args: Array[String]): Unit =
    (new HelloWorld).start()
}

class HelloWorld {
  val system = ActorSystem("HelloWorld")
  implicit val materializer = ActorMaterializer.create(system)
  import system.dispatcher

  def start(): Unit = {

    val serverHandler = new AbstractReactiveSocketScaladsl {
      override def requestResponse(payload: Payload): Future[Payload] = {
        println(s"# got request") // FIXME
        Future.successful(payload)
      }
    }
    val server = ReactiveSocketServer.create(TcpTransportServer.create())
      .start(SocketAcceptorAdapter.disabledLease(serverHandler))

    val serverAddress = server.getServerAddress
    println(s"started server: $serverAddress") // FIXME

    val client: Future[ReactiveSocketScaladsl] =
      ReactiveSocketClientScaladsl(ReactiveSocketClient.create(TcpTransportClient.create(serverAddress),
        keepAlive(never()).disableLease()))

    client.foreach { socket =>
      println(s"# got socket") // FIXME
      val response: Future[Payload] = socket.requestResponse(new PayloadImpl("Hello"))

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

}

trait ReactiveSocketScaladsl {

  def fireAndForget(payload: Payload): Future[Done]

  def requestResponse(payload: Payload): Future[Payload]

  def requestStream(payload: Payload): Source[Payload, NotUsed]

  def requestChannel(payloads: Source[Payload, NotUsed]): Source[Payload, NotUsed]

  def close(): Unit

}

trait AbstractReactiveSocketScaladsl extends ReactiveSocketScaladsl {

  // FIXME real, empty implementations

  def fireAndForget(payload: Payload): Future[Done] = ???

  def requestResponse(payload: Payload): Future[Payload] = ???

  def requestStream(payload: Payload): Source[Payload, NotUsed] = ???

  def requestChannel(payloads: Source[Payload, NotUsed]): Source[Payload, NotUsed] = ???

  def close(): Unit = ???

}

object SocketAcceptorAdapter {

  def disabledLease(handler: ReactiveSocketScaladsl)(implicit materializer: Materializer): SocketAcceptor =
    apply((_, _) => new DisabledLeaseAcceptingSocket(ReactiveSocketAdapter(handler)))

  // FIXME add more factory methods for other common lease types

  /**
   * For full control
   */
  def apply(f: Function2[ConnectionSetupPayload, ReactiveSocket, LeaseEnforcingSocket]): SocketAcceptor = {
    new SocketAcceptor {
      override def accept(setup: ConnectionSetupPayload, sendingSocket: ReactiveSocket): LeaseEnforcingSocket =
        f(setup, sendingSocket)
    }
  }

}

object ReactiveSocketAdapter {
  def apply(delegate: ReactiveSocketScaladsl)(implicit materializer: Materializer): ReactiveSocketAdapter =
    new ReactiveSocketAdapter(delegate)(materializer)
}

class ReactiveSocketAdapter(delegate: ReactiveSocketScaladsl)(implicit materializer: Materializer)
  extends AbstractReactiveSocket {

  override def requestResponse(payload: Payload): Publisher[Payload] =
    Source.fromFuture(delegate.requestResponse(payload)).runWith(Sink.asPublisher(fanout = false))

  // FIXME delegate other methods

}

object ReactiveSocketClientScaladsl {
  def apply(client: ReactiveSocketClient)(implicit materializer: Materializer): Future[ReactiveSocketScaladsl] = {
    val clientPublisher = client.connect()
    implicit val ec = materializer.executionContext
    Source.fromPublisher(clientPublisher).runWith(Sink.head).map(new ReactiveSocketClientScaladsl(_))
  }
}

class ReactiveSocketClientScaladsl(socket: ReactiveSocket)(implicit materializer: Materializer)
  extends ReactiveSocketScaladsl {

  override def fireAndForget(payload: Payload): Future[Done] = ???

  override def requestResponse(payload: Payload): Future[Payload] =
    Source.fromPublisher(socket.requestResponse(payload)).runWith(Sink.head)

  override def requestStream(payload: Payload): Source[Payload, NotUsed] = ???

  override def requestChannel(payloads: Source[Payload, NotUsed]): Source[Payload, NotUsed] = ???

  override def close(): Unit = ???

}

