/*
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.reactivesocket.scaladsl

import java.net.SocketAddress

import akka.{ Done, NotUsed }
import akka.stream.scaladsl.{ Flow, Source }
import io.reactivesocket.transport.{ TransportClient, TransportServer }
import io.reactivesocket.{ Payload, ReactiveSocket }

import scala.concurrent.Future

object ReactiveSocketServer {

  case class IncomingConnection(flow: Flow[Payload, Payload, NotUsed])
  case class Binding(localAddress: SocketAddress)

  def apply(ts: TransportServer): Source[IncomingConnection, Binding] = ???

}

object ReactiveSocketClient {

  def fireAndForget(p: Payload)(implicit rs: ReactiveSocket): Future[Done] = ???

  def singleRequest(p: Payload)(implicit rs: ReactiveSocket): Future[Payload] = ???

  def stream(p: Payload)(implicit rs: ReactiveSocket): Source[Payload, NotUsed] = ???

  def channel()(implicit rs: ReactiveSocket): Flow[Payload, Payload, NotUsed] = ???

}

object ReactiveSocket {

  def apply(tc: TransportClient): ReactiveSocket = ???

}
