/*
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.reactivesocket.scaladsl

import java.util.concurrent.TimeUnit
import java.util.function.IntSupplier

import io.reactivesocket.transport.tcp.server.TcpTransportServer
import io.reactivesocket.server.ReactiveSocketServer
import io.reactivesocket.server.ReactiveSocketServer.SocketAcceptor
import io.reactivesocket.ConnectionSetupPayload
import io.reactivesocket.lease.{ DefaultLeaseEnforcingSocket, DisabledLeaseAcceptingSocket, FairLeaseDistributor, LeaseEnforcingSocket }
import io.reactivesocket.ReactiveSocket
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

import scala.concurrent.duration._

object HelloWorld {
  def main(args: Array[String]): Unit =
    (new HelloWorld).start()
}

class HelloWorld {
  val system = ActorSystem("HelloWorld")
  implicit val materializer = ActorMaterializer.create(system)
  import system.dispatcher

  def start(): Unit = {

    val serverHandler = new AbstractReactiveSocketApi {
      override def requestResponse(payload: Payload): Future[Payload] = {
        println(s"# got request") // FIXME
        Future.successful(payload)
      }
    }
    val server = ReactiveSocketServer.create(TcpTransportServer.create())
      .start(ServerSocketAcceptorAdapter.defaultLease(setup => serverHandler))

    val serverAddress = server.getServerAddress
    println(s"started server: $serverAddress") // FIXME

    val client: Future[ReactiveSocketApi] =
      ClientReactiveSocketAdapter(ReactiveSocketClient.create(
        TcpTransportClient.create(serverAddress),
        keepAlive(never()) //.disableLease()
      ))

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

trait ReactiveSocketApi {

  def fireAndForget(payload: Payload): Future[Done]

  def requestResponse(payload: Payload): Future[Payload]

  def requestStream(payload: Payload): Source[Payload, NotUsed]

  def requestChannel(payloads: Source[Payload, NotUsed]): Source[Payload, NotUsed]

  def close(): Unit

}

trait AbstractReactiveSocketApi extends ReactiveSocketApi {

  // FIXME real, empty implementations

  def fireAndForget(payload: Payload): Future[Done] = ???

  def requestResponse(payload: Payload): Future[Payload] = ???

  def requestStream(payload: Payload): Source[Payload, NotUsed] = ???

  def requestChannel(payloads: Source[Payload, NotUsed]): Source[Payload, NotUsed] = ???

  def close(): Unit = ???

}

object ServerSocketAcceptorAdapter {

  def disabledLease(handler: ConnectionSetupPayload => ReactiveSocketApi)(implicit materializer: Materializer): SocketAcceptor =
    apply((setup, _) => new DisabledLeaseAcceptingSocket(ReactiveSocketAdapter(handler(setup))))

  def defaultLease(handler: ConnectionSetupPayload => ReactiveSocketApi)(implicit materializer: Materializer): SocketAcceptor = {
    val lease = Source.tick(0.seconds, 30.seconds, 10L.asInstanceOf[java.lang.Long]).runWith(Sink.asPublisher(false))
    val leaseDistributor = new FairLeaseDistributor(new IntSupplier { def getAsInt = 5000 }, 5000, lease)
    apply((setup, _) => new DefaultLeaseEnforcingSocket(ReactiveSocketAdapter(handler(setup)), leaseDistributor))
  }

  //DefaultLeaseEnforcingSocket

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
  def apply(delegate: ReactiveSocketApi)(implicit materializer: Materializer): ReactiveSocketAdapter =
    new ReactiveSocketAdapter(delegate)(materializer)
}

class ReactiveSocketAdapter(delegate: ReactiveSocketApi)(implicit materializer: Materializer)
  extends AbstractReactiveSocket {

  override def requestResponse(payload: Payload): Publisher[Payload] =
    Source.fromFuture(delegate.requestResponse(payload)).runWith(Sink.asPublisher(fanout = false))

  // FIXME delegate other methods

}

object ClientReactiveSocketAdapter {
  def apply(underlyingClient: ReactiveSocketClient)(implicit materializer: Materializer): Future[ReactiveSocketApi] = {
    val clientPublisher = underlyingClient.connect()
    implicit val ec = materializer.executionContext
    Source.fromPublisher(clientPublisher).runWith(Sink.head).map(new ClientReactiveSocketAdapter(_))
  }
}

class ClientReactiveSocketAdapter(underlyingSocket: ReactiveSocket)(implicit materializer: Materializer)
  extends ReactiveSocketApi {

  override def fireAndForget(payload: Payload): Future[Done] = ???

  override def requestResponse(payload: Payload): Future[Payload] =
    Source.fromPublisher(underlyingSocket.requestResponse(payload)).runWith(Sink.head)

  override def requestStream(payload: Payload): Source[Payload, NotUsed] = ???

  override def requestChannel(payloads: Source[Payload, NotUsed]): Source[Payload, NotUsed] = ???

  override def close(): Unit = {
    println("closing")
  }

}
