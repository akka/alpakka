/*
 * Copyright (C) since 2016 Lightbend Inc. <https://akka.io>
 */

package akka.stream.alpakka.google.firebase.fcm.v1

import akka.stream.alpakka.google.firebase.fcm.v1.models.{Condition, FcmNotification, Token, Topic}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class FcmNotificationSpec extends AnyWordSpec with Matchers {

  "SendableNotification" should {

    "target is mandatory" must {
      "not fail if only one target added" in {
        FcmNotification(token = Option("")).isSendable shouldBe true
        FcmNotification(topic = Option("")).isSendable shouldBe true
        FcmNotification(condition = Option("")).isSendable shouldBe true
      }

      "must fail if two target added" in {
        FcmNotification(token = Option(""), topic = Option("")).isSendable shouldBe false
        FcmNotification(token = Option(""), condition = Option("")).isSendable shouldBe false
        FcmNotification(topic = Option(""), condition = Option("")).isSendable shouldBe false
      }

      "must fail if all target added" in {
        FcmNotification(token = Option(""), topic = Option(""), condition = Option("")).isSendable shouldBe false
      }
    }

    "withTarget don't build invalid objects" in {
      val original = FcmNotification(token = Option(""))
      val first = original.withTarget(Topic(""))
      val second = first.withTarget(Condition(Condition.Topic("")))
      val third = second.withTarget(Token(""))
      original.isSendable shouldBe true
      first.isSendable shouldBe true
      second.isSendable shouldBe true
      third.isSendable shouldBe true
    }

  }
}
