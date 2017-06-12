/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.kairosdb

import org.scalatest.{FlatSpec, Matchers}

class KairosSinkSettingsSpec extends FlatSpec with Matchers {
  it should "require parallelism maxInFlight" in {
    a[IllegalArgumentException] should be thrownBy {
      KairosSinkSettings(0)
    }
  }

  it should "accept valid parameters" in {
    KairosSinkSettings(1)
  }
}
