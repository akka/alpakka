/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.backblazeb2.scaladsl

import com.github.tomakehurst.wiremock.WireMockServer
import com.github.tomakehurst.wiremock.client.WireMock
import com.github.tomakehurst.wiremock.common.ConsoleNotifier
import com.github.tomakehurst.wiremock.core.WireMockConfiguration.options
import com.typesafe.config.{Config, ConfigFactory}
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike}

abstract class WireMockBase extends FlatSpecLike with BeforeAndAfterAll {
  val mockServer = createMockServer()
  val mock = new WireMock("localhost", mockServer.port())

  protected val config: Config = ConfigFactory.parseString(
    s"""
       |akka {
       |  ssl-config.trustManager.stores = [
       |        {type = "PEM", path = "./backblazeb2/src/test/resources/rootCA.crt"}
       |      ]
       |}
    """.stripMargin
  )

  private def createMockServer(): WireMockServer = {
    val server = new WireMockServer(
      options()
        .notifier(new ConsoleNotifier(false))
        .dynamicPort()
        .dynamicHttpsPort()
        .keystorePath("./backblazeb2/src/test/resources/keystore.jks")
        .keystorePassword("abcdefg")
    )

    server.start()
    server
  }

  override def afterAll(): Unit = mockServer.stop()
}
