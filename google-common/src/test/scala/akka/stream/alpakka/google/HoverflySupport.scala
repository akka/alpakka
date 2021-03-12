/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.google

import io.specto.hoverfly.junit.core.{Hoverfly, HoverflyConfig, HoverflyMode}
import org.scalatest.{BeforeAndAfterAll, Suite}

trait HoverflySupport extends BeforeAndAfterAll { this: Suite =>

  def hoverfly = BigQueryHoverfly

  override def beforeAll(): Unit = {
    super.beforeAll()
    hoverfly.start()
  }

  override def afterAll(): Unit = {
    super.afterAll()
    hoverfly.close()
  }
}

object BigQueryHoverfly
    extends Hoverfly(
      HoverflyConfig
        .localConfigs()
        .proxyPort(8500)
        .adminPort(8888)
        .captureHeaders("Content-Range", "X-Upload-Content-Type")
        .enableStatefulCapture(),
      HoverflyMode.SIMULATE
    ) {
  def getInstance = this
}
