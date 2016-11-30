/*
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.s3.impl

import akka.actor.ActorSystem
import akka.stream.alpakka.s3.impl.WireMockBase._
import akka.testkit.TestKit
import com.github.tomakehurst.wiremock.WireMockServer
import com.github.tomakehurst.wiremock.client.WireMock
import com.github.tomakehurst.wiremock.client.WireMock._
import com.github.tomakehurst.wiremock.core.WireMockConfiguration._
import com.typesafe.config.ConfigFactory
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike}
import scala.concurrent.duration._

object WireMockBase {
  def config(port: Int) = ConfigFactory.parseString(
    s"""
    |akka {
    |  loggers = ["akka.testkit.TestEventListener"]
    |
    |  ssl-config.trustManager.stores = [
    |        {type = "PEM", path = "./s3/src/test/resources/rootCA.crt"}
    |      ]
    |  stream.alpakka.s3.proxy {
    |   host = localhost
    |   port = $port
    | }
    |}
  """.stripMargin
  )

  def getCallerName(clazz: Class[_]): String = {
    val s = (Thread.currentThread.getStackTrace map (_.getClassName) drop 1).dropWhile(
        _ matches "(java.lang.Thread|.*WireMockBase.?$)")
    val reduced = s.lastIndexWhere(_ == clazz.getName) match {
      case -1 ⇒ s
      case z ⇒ s drop (z + 1)
    }
    reduced.head.replaceFirst(""".*\.""", "").replaceAll("[^a-zA-Z_0-9]", "_")
  }

  def initServer(): WireMockServer = {
    val server = new WireMockServer(
        wireMockConfig()
          .dynamicPort()
          .dynamicHttpsPort()
          .keystorePath("./s3/src/test/resources/keystore.jks")
          .keystorePassword("abcdefg"))
    server.start()
    server
  }
}

abstract class WireMockBase(_system: ActorSystem, _wireMockServer: WireMockServer) extends TestKit(_system)
    with FlatSpecLike with BeforeAndAfterAll {

  def this(mock: WireMockServer) = this(ActorSystem(getCallerName(getClass), config(mock.httpsPort())), mock)
  def this() = this(initServer())

  val mock = new WireMock("localhost", _wireMockServer.port())

  override def beforeAll(): Unit =
    mock.register(
        any(anyUrl()).willReturn(
            aResponse()
              .withStatus(404)
              .withBody("<Error><Code>NoSuchKey</Code><Message>No key found</Message>" +
                "<RequestId>XXXX</RequestId><HostId>XXXX</HostId></Error>")))
  override def afterAll(): Unit = _wireMockServer.stop()

}
