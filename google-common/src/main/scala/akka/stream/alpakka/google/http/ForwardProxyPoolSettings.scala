/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.google.http

import akka.actor.ActorSystem
import akka.annotation.InternalApi
import akka.http.scaladsl.ClientTransport
import akka.http.scaladsl.Http.OutgoingConnection
import akka.http.scaladsl.model.headers.BasicHttpCredentials
import akka.http.scaladsl.settings.{ClientConnectionSettings, ConnectionPoolSettings}
import akka.stream.scaladsl.{Flow, Tcp}
import akka.util.ByteString

import java.net.InetSocketAddress
import scala.concurrent.Future

@InternalApi
private[google] object ForwardProxyPoolSettings {

  def apply(scheme: String, host: String, port: Int, credentials: Option[BasicHttpCredentials])(
      implicit system: ActorSystem
  ): ConnectionPoolSettings = {
    val address = InetSocketAddress.createUnresolved(host, port)
    val transport = scheme match {
      case "https" =>
        credentials.fold(ClientTransport.httpsProxy(address))(ClientTransport.httpsProxy(address, _))
      case "http" =>
        new ChangeTargetEndpointTransport(address)
      case _ =>
        throw new IllegalArgumentException("scheme must be either `http` or `https`")
    }
    ConnectionPoolSettings(system)
      .withConnectionSettings(
        ClientConnectionSettings(system)
          .withTransport(transport)
      )
  }

}

private[http] final class ChangeTargetEndpointTransport(address: InetSocketAddress) extends ClientTransport {
  def connectTo(ignoredHost: String, ignoredPort: Int, settings: ClientConnectionSettings)(
      implicit system: ActorSystem
  ): Flow[ByteString, ByteString, Future[OutgoingConnection]] =
    Tcp()
      .outgoingConnection(address,
                          settings.localAddress,
                          settings.socketOptions,
                          halfClose = true,
                          settings.connectingTimeout,
                          settings.idleTimeout)
      .mapMaterializedValue(
        _.map(tcpConn => OutgoingConnection(tcpConn.localAddress, tcpConn.remoteAddress))(system.dispatcher)
      )
}
