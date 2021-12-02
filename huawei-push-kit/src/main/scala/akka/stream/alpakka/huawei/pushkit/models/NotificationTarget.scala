/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.huawei.pushkit.models

sealed trait NotificationTarget

/**
 * Tokens model.
 */
case class Tokens(token: Seq[String]) extends NotificationTarget

/**
 * Topic model.
 */
case class Topic(topic: String) extends NotificationTarget

/**
 * Condition model.
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
