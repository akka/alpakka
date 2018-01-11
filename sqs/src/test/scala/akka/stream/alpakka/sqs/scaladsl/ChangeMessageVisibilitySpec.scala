/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.sqs.scaladsl

import akka.stream.alpakka.sqs.MessageAction
import org.scalatest.{FlatSpec, Matchers}

class ChangeMessageVisibilitySpec extends FlatSpec with Matchers {

  it should "require valid visibility" in {
    a[IllegalArgumentException] should be thrownBy {
      MessageAction.ChangeMessageVisibility(43201)
    }
    a[IllegalArgumentException] should be thrownBy {
      MessageAction.ChangeMessageVisibility(-1)
    }
  }

  it should "accept valid parameters" in {
    MessageAction.ChangeMessageVisibility(300)
  }

  it should "allow terminating visibility" in {
    MessageAction.ChangeMessageVisibility(0)
  }
}
