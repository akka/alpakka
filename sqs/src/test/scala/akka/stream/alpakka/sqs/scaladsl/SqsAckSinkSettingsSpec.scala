/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.sqs.scaladsl

import akka.stream.alpakka.sqs.SqsAckSinkSettings
import org.scalatest.{FlatSpec, Matchers}

class SqsAckSinkSettingsSpec extends FlatSpec with Matchers {

  it should "require valid maxInFlight" in {
    a[IllegalArgumentException] should be thrownBy {
      SqsAckSinkSettings(0)
    }
  }

  it should "accept valid parameters" in {
    SqsAckSinkSettings(1)
  }
}
