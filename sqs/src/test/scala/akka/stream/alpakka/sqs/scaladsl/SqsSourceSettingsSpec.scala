/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.sqs.scaladsl

import akka.stream.alpakka.sqs.SqsSourceSettings
import org.scalatest.{FlatSpec, Matchers}

class SqsSourceSettingsSpec extends FlatSpec with Matchers {

  it should "accept valid parameters" in {
    SqsSourceSettings(waitTimeSeconds = 1, maxBatchSize = 2, maxBufferSize = 3, attributeNames = Seq("All"))
  }

  it should "require maxBatchSize <= maxBufferSize" in {
    a[IllegalArgumentException] should be thrownBy {
      SqsSourceSettings(waitTimeSeconds = 1, maxBatchSize = 5, maxBufferSize = 3)
    }
  }

  it should "require AttributeName to be valid" in {
    a[IllegalArgumentException] should be thrownBy {
      SqsSourceSettings(waitTimeSeconds = 1, maxBatchSize = 2, maxBufferSize = 3, attributeNames = Seq("Test"))
    }
  }

  it should "require MessageAttributeName to meet AWS requirements" in {
    a[IllegalArgumentException] should be thrownBy {
      SqsSourceSettings(waitTimeSeconds = 1,
                        maxBatchSize = 2,
                        maxBufferSize = 3,
                        messageAttributeNames = Seq(".failed"))
    }

    a[IllegalArgumentException] should be thrownBy {
      SqsSourceSettings(waitTimeSeconds = 1,
                        maxBatchSize = 2,
                        maxBufferSize = 3,
                        messageAttributeNames = Seq("failed."))
    }

    a[IllegalArgumentException] should be thrownBy {
      SqsSourceSettings(
        waitTimeSeconds = 1,
        maxBatchSize = 2,
        maxBufferSize = 3,
        messageAttributeNames = Seq(
          "A.really.realy.long.attribute.name.that.is.longer.than.what.is.allowed.256.characters.are.allowed." +
          "however.they.cannot.contain.anything.other.than.alphanumerics.hypens.underscores.and.periods.though" +
          "you.cant.have.more.than.one.consecutive.period.they.are.also.case.sensitive"
        )
      )
    }

    a[IllegalArgumentException] should be thrownBy {
      SqsSourceSettings(waitTimeSeconds = 1,
                        maxBatchSize = 2,
                        maxBufferSize = 3,
                        messageAttributeNames = Seq("multiple..periods"))
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
