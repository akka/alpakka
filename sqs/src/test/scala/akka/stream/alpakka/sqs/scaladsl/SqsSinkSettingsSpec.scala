/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.sqs.scaladsl

import akka.stream.alpakka.sqs.SqsSinkSettings
import org.scalatest.{FlatSpec, Matchers}

class SqsSinkSettingsSpec extends FlatSpec with Matchers {

  it should "require valid maxInFlight" in {
    a[IllegalArgumentException] should be thrownBy {
      SqsSinkSettings(0)
    }
  }

  it should "accept valid parameters" in {
    SqsSinkSettings(1)
  }
}
