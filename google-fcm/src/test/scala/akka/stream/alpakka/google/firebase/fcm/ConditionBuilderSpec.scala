/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.google.firebase.fcm

import akka.stream.alpakka.google.firebase.fcm.FcmNotificationModels.Condition._
import org.scalatest.{Matchers, WordSpecLike}

class ConditionBuilderSpec extends WordSpecLike with Matchers {

  "ConditionBuilder" must {

    "serialize Topic as expected" in {
      Topic("TopicA").toConditionText shouldBe """'TopicA' in topics"""
    }

    "serialize And as expected" in {
      And(Topic("TopicA"), Topic("TopicB")).toConditionText shouldBe """('TopicA' in topics && 'TopicB' in topics)"""
    }

    "serialize Or as expected" in {
      Or(Topic("TopicA"), Topic("TopicB")).toConditionText shouldBe """('TopicA' in topics || 'TopicB' in topics)"""
    }

    "serialize Not as expected" in {
      Not(Topic("TopicA")).toConditionText shouldBe """!('TopicA' in topics)"""
    }

    "serialize recursively and stay correct" in {
      And(Or(Topic("TopicA"), Topic("TopicB")), Or(Topic("TopicC"), Not(Topic("TopicD")))).toConditionText shouldBe
      """(('TopicA' in topics || 'TopicB' in topics) && ('TopicC' in topics || !('TopicD' in topics)))"""
    }

    "can use cool operators" in {
      (Topic("TopicA") && (Topic("TopicB") || (Topic("TopicC") && !Topic("TopicD")))).toConditionText shouldBe
      """('TopicA' in topics && ('TopicB' in topics || ('TopicC' in topics && !('TopicD' in topics))))"""
    }
  }

}
