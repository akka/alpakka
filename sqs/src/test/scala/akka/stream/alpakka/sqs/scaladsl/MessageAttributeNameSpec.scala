/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.sqs.scaladsl

import akka.stream.alpakka.sqs.MessageAttributeName
import akka.stream.alpakka.testkit.scaladsl.LogCapturing
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class MessageAttributeNameSpec extends AnyFlatSpec with Matchers with LogCapturing {

  it should "not allow names which have periods at the beginning" in {
    a[IllegalArgumentException] should be thrownBy {
      MessageAttributeName(".failed")
    }
  }

  it should "not allow names which have periods at the end" in {
    a[IllegalArgumentException] should be thrownBy {
      MessageAttributeName("failed.")
    }

  }

  it should "reject names which are longer than 256 characters" in {
    a[IllegalArgumentException] should be thrownBy {
      MessageAttributeName(
        "A.really.realy.long.attribute.name.that.is.longer.than.what.is.allowed.256.characters.are.allowed." +
        "however.they.cannot.contain.anything.other.than.alphanumerics.hypens.underscores.and.periods.though" +
        "you.cant.have.more.than.one.consecutive.period.they.are.also.case.sensitive"
      )
    }
  }
  it should "reject names with multiple sequential periods" in {
    a[IllegalArgumentException] should be thrownBy {
      MessageAttributeName("multiple..periods")
    }
  }

}
