/*
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.s3.impl

import akka.actor.ActorSystem
import akka.testkit.TestKit
import com.github.tomakehurst.wiremock.core.WireMockConfiguration._
import com.github.tomakehurst.wiremock.junit.WireMockRule
import com.github.tomakehurst.wiremock.client.WireMock._
import com.typesafe.config.ConfigFactory
import org.scalatest.{ BeforeAndAfterAll, FlatSpecLike }

object WireMockBase {
  val config = ConfigFactory.parseString(
    """
    |akka {
    |  loggers = ["akka.testkit.TestEventListener"]
    |
    |  ssl-config.trustManager.stores = [
    |        {type = "PEM", path = "./s3/src/test/resources/rootCA.crt"}
    |      ]
    |   stream.alpakka.s3.proxy {
    |     host = localhost
    |     port = 8443
    |   }
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
}

abstract class WireMockBase(_system: ActorSystem) extends TestKit(_system) with FlatSpecLike with BeforeAndAfterAll {
  def this() = this(ActorSystem(WireMockBase.getCallerName(getClass), ConfigFactory.load(WireMockBase.config)))

  val wireMockServer = new WireMockRule(
      wireMockConfig()
        .port(8080)
        .httpsPort(8443)
        .keystorePath("./s3/src/test/resources/keystore.jks")
        .keystorePassword("abcdefg"))

  override def beforeAll(): Unit = {
    wireMockServer.start()
    stubFor(
        any(anyUrl()).willReturn(
            aResponse()
              .withStatus(404)
              .withBody("<Error><Code>NoSuchKey</Code><Message>No key found</Message>" +
                "<RequestId>XXXX</RequestId><HostId>XXXX</HostId></Error>")))
  }

  override def afterAll(): Unit = wireMockServer.stop()

}
