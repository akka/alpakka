/*
 * Copyright (C) since 2016 Lightbend Inc. <https://akka.io>
 */

package akka.stream.alpakka.huawei.pushkit

import akka.actor.ActorSystem
import akka.annotation.InternalApi
import akka.http.scaladsl.ClientTransport
import akka.http.scaladsl.model.headers.BasicHttpCredentials
import akka.http.scaladsl.settings.{ClientConnectionSettings, ConnectionPoolSettings}

import java.net.InetSocketAddress

/**
 * INTERNAL API
 */
@InternalApi
private[pushkit] object ForwardProxyPoolSettings {

  implicit class ForwardProxyPoolSettings(forwardProxy: ForwardProxy) {

    def poolSettings(system: ActorSystem) = {
      val address = InetSocketAddress.createUnresolved(forwardProxy.host, forwardProxy.port)
      val transport = forwardProxy.credentials.fold(ClientTransport.httpsProxy(address))(
        c => ClientTransport.httpsProxy(address, BasicHttpCredentials(c.username, c.password))
      )

      ConnectionPoolSettings(system)
        .withConnectionSettings(
          ClientConnectionSettings(system)
            .withTransport(transport)
        )
    }
  }

}
