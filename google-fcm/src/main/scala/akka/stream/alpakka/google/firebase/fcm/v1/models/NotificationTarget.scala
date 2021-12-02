/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.google.firebase.fcm.v1.models

sealed trait NotificationTarget

/**
 * Token model.
 * @see https://firebase.google.com/docs/reference/fcm/rest/v1/projects.messages
 */
case class Token(token: String) extends NotificationTarget

/**
 * Topic model.
 * @see https://firebase.google.com/docs/reference/fcm/rest/v1/projects.messages
 */
case class Topic(topic: String) extends NotificationTarget

/**
 * Condition model.
 * @see https://firebase.google.com/docs/reference/fcm/rest/v1/projects.messages
 */
case class Condition(conditionText: String) extends NotificationTarget

object Condition {
  sealed trait ConditionBuilder {
    def &&(condition: ConditionBuilder) = And(this, condition)
    def ||(condition: ConditionBuilder) = Or(this, condition)
    def unary_! = Not(this)
    def toConditionText: String
  }
  case class Topic(topic: String) extends ConditionBuilder {
    def toConditionText: String = s"'$topic' in topics"
  }
  case class And(condition1: ConditionBuilder, condition2: ConditionBuilder) extends ConditionBuilder {
    def toConditionText: String = s"(${condition1.toConditionText} && ${condition2.toConditionText})"
  }
  case class Or(condition1: ConditionBuilder, condition2: ConditionBuilder) extends ConditionBuilder {
    def toConditionText: String = s"(${condition1.toConditionText} || ${condition2.toConditionText})"
  }
  case class Not(condition: ConditionBuilder) extends ConditionBuilder {
    def toConditionText: String = s"!(${condition.toConditionText})"
  }

  def apply(builder: ConditionBuilder): Condition =
    Condition(builder.toConditionText)
}
