/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.logging

import com.typesafe.config.ConfigFactory
import io.specto.hoverfly.junit.core.{Hoverfly, HoverflyConfig, HoverflyMode}
import org.scalatest.{BeforeAndAfterAll, Suite}

object HoverflySupport {

  val config = ConfigFactory.parseString("""
      |alpakka.google {
      |  forward-proxy {
      |    host = localhost
      |    port = 8500
      |    trust-pem = "src/test/resources/cert.pem"
      |  }
      |}
      |""".stripMargin)
}

trait HoverflySupport extends BeforeAndAfterAll { this: Suite =>

  def hoverfly = CloudLoggingHoverfly

  override def beforeAll(): Unit = {
    super.beforeAll()
    hoverfly.start()
  }

  override def afterAll(): Unit = {
    super.afterAll()
    hoverfly.close()
  }
}

object CloudLoggingHoverfly
    extends Hoverfly(
      HoverflyConfig
        .localConfigs()
        .proxyPort(8500)
        .adminPort(8888)
        .enableStatefulCapture(),
      HoverflyMode.SIMULATE
    ) {
  def getInstance = this
}
