/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.sqs.scaladsl

import akka.stream.alpakka.sqs.{All, SqsSourceSettings}
import org.scalatest.{FlatSpec, Matchers}

class SqsSourceSettingsSpec extends FlatSpec with Matchers {

  it should "accept valid parameters" in {
    SqsSourceSettings(waitTimeSeconds = 1, maxBatchSize = 2, maxBufferSize = 3, attributeNames = Seq(All))
  }

  it should "require maxBatchSize <= maxBufferSize" in {
    a[IllegalArgumentException] should be thrownBy {
      SqsSourceSettings(waitTimeSeconds = 1, maxBatchSize = 5, maxBufferSize = 3)
    }
  }

  it should "require waitTimeSeconds within AWS SQS limits" in {
    a[IllegalArgumentException] should be thrownBy {
      SqsSourceSettings(waitTimeSeconds = -1, maxBatchSize = 1, maxBufferSize = 2)
    }

    a[IllegalArgumentException] should be thrownBy {
      SqsSourceSettings(waitTimeSeconds = 100, maxBatchSize = 1, maxBufferSize = 2)
    }
  }

  it should "require maxBatchSize within AWS SQS limits" in {
    a[IllegalArgumentException] should be thrownBy {
      SqsSourceSettings(waitTimeSeconds = 5, maxBatchSize = 0, maxBufferSize = 2)
    }

    a[IllegalArgumentException] should be thrownBy {
      SqsSourceSettings(waitTimeSeconds = 5, maxBatchSize = 11, maxBufferSize = 2)
    }
  }
}
