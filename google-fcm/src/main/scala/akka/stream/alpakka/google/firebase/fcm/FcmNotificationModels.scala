/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.google.firebase.fcm

import akka.stream.alpakka.google.firebase.fcm.FcmNotificationModels._

object FcmNotificationModels {

  case class BasicNotification(title: String, body: String)

  case class AndroidNotification(
      title: String,
      body: String,
      icon: String,
      color: String,
      sound: String,
      tag: String,
      click_action: String,
      body_loc_key: String,
      body_loc_args: Seq[String],
      title_loc_key: String,
      title_loc_args: Seq[String]
  )

  case class AndroidConfig(
      collapse_key: String,
      priority: AndroidMessagePriority,
      ttl: String,
      restricted_package_name: String,
      data: Map[String, String],
      notification: AndroidNotification
  )

  sealed trait AndroidMessagePriority
  case object Normal extends AndroidMessagePriority
  case object High extends AndroidMessagePriority

  case class WebPushNotification(title: String, body: String, icon: String)
  case class WebPushConfig(headers: Map[String, String], data: Map[String, String], notification: WebPushNotification)

  case class ApnsConfig(headers: Map[String, String], rawPayload: String)

  sealed trait NotificationTarget
  case class Token(token: String) extends NotificationTarget
  case class Topic(topic: String) extends NotificationTarget
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
}

case class FcmNotification(
    data: Option[Map[String, String]] = None,
    notification: Option[BasicNotification] = None,
    android: Option[AndroidConfig] = None,
    webPush: Option[WebPushConfig] = None,
    apns: Option[ApnsConfig] = None,
    token: Option[String] = None,
    topic: Option[String] = None,
    condition: Option[String] = None
) {
  def withData(data: Map[String, String]): FcmNotification = this.copy(data = Option(data))
  def withBasicNotification(notification: BasicNotification): FcmNotification =
    this.copy(notification = Option(notification))
  def withBasicNotification(title: String, body: String): FcmNotification =
    this.copy(notification = Option(BasicNotification(title, body)))
  def withAndroidConfig(android: AndroidConfig): FcmNotification = this.copy(android = Option(android))
  def withWebPushConfig(webPush: WebPushConfig): FcmNotification = this.copy(webPush = Option(webPush))
  def withApnsConfig(apns: ApnsConfig): FcmNotification = this.copy(apns = Option(apns))
  def withTarget(target: NotificationTarget): FcmNotification = target match {
    case Token(t) => this.copy(token = Option(t), topic = None, condition = None)
    case Topic(t) => this.copy(token = None, topic = Option(t), condition = None)
    case Condition(t) => this.copy(token = None, topic = None, condition = Option(t))
  }
  def isSendable: Boolean =
    (token.isDefined ^ topic.isDefined ^ condition.isDefined) && !(token.isDefined && topic.isDefined)
}

object FcmNotification {
  val empty: FcmNotification = FcmNotification()
  def fromJava(): FcmNotification = empty
  def apply(notification: BasicNotification, target: NotificationTarget): FcmNotification =
    empty.withBasicNotification(notification).withTarget(target)
  def apply(title: String, body: String, target: NotificationTarget): FcmNotification =
    empty.withBasicNotification(title, body).withTarget(target)
  def basic(title: String, body: String, target: NotificationTarget) = FcmNotification(title, body, target)
}

sealed trait FcmResponse {
  def isFailure: Boolean
  def isSuccess: Boolean
}

final case class FcmSuccessResponse(name: String) extends FcmResponse {
  val isFailure = false
  val isSuccess = true
  def getName: String = name
}

final case class FcmErrorResponse(rawError: String) extends FcmResponse {
  val isFailure = true
  val isSuccess = false
  def getRawError: String = rawError
}
