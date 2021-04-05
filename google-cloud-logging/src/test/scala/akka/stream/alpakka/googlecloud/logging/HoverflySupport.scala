/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.logging

import io.specto.hoverfly.junit.core.{Hoverfly, HoverflyConfig, HoverflyMode}
import org.scalatest.{BeforeAndAfterAll, Suite}

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
