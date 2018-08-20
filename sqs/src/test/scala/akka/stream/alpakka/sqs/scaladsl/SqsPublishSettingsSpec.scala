/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.sqs.scaladsl

import akka.stream.alpakka.sqs.SqsPublishSettings
import org.scalatest.{FlatSpec, Matchers}

class SqsPublishSettingsSpec extends FlatSpec with Matchers {

  it should "require valid maxInFlight" in {
    a[IllegalArgumentException] should be thrownBy {
      SqsPublishSettings(0)
    }
  }

  it should "accept valid parameters" in {
    SqsPublishSettings(1)
  }
}
