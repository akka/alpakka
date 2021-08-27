/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.google.firebase.fcm.v1.models

/**
 * Firebase Cloud Message model.
 * @see https://firebase.google.com/docs/reference/fcm/rest/v1/projects.messages
 */
case class FcmNotification(
    data: Option[Map[String, String]] = None,
    notification: Option[BasicNotification] = None,
    android: Option[AndroidConfig] = None,
    webpush: Option[WebPushConfig] = None,
    apns: Option[ApnsConfig] = None,
    fcm_options: Option[FcmOption] = None,
    token: Option[String] = None,
    topic: Option[String] = None,
    condition: Option[String] = None
) {
  def withTarget(target: NotificationTarget): FcmNotification = target match {
    case Token(t) => this.copy(token = Option(t), topic = None, condition = None)
    case Topic(t) => this.copy(token = None, topic = Option(t), condition = None)
    case Condition(t) => this.copy(token = None, topic = None, condition = Option(t))
  }
  def isSendable: Boolean =
    (token.isDefined ^ topic.isDefined ^ condition.isDefined) && !(token.isDefined && topic.isDefined)
  def withBasicNotification(title: String, body: String): FcmNotification =
    this.copy(notification = Option(BasicNotification(title, body, None)))
  def withBasicNotification(title: String, body: String, image: String): FcmNotification =
    this.copy(notification = Option(BasicNotification(title, body, Option(image))))
  def withBasicNotification(notification: BasicNotification): FcmNotification =
    this.copy(notification = Option(notification))
  def withData(data: Map[String, String]): FcmNotification = this.copy(data = Option(data))
  def withApnsConfig(apns: ApnsConfig): FcmNotification = this.copy(apns = Option(apns))
  def withWebPushConfig(webPush: WebPushConfig): FcmNotification = this.copy(webpush = Option(webPush))
  def withAndroidConfig(android: AndroidConfig): FcmNotification = this.copy(android = Option(android))
  def withFcmOptions(fcm_options: FcmOptions): FcmNotification =
    this.copy(fcm_options = Option(fcm_options))
  def withFcmOptions(fcm_options: String): FcmNotification =
    this.copy(fcm_options = Option(FcmOptions(fcm_options)))
}

object FcmNotification {
  val empty: FcmNotification = FcmNotification()
  def fromJava(): FcmNotification = empty
  def apply(title: String, body: String, target: NotificationTarget): FcmNotification =
    empty.withBasicNotification(title, body).withTarget(target)
  def basic(title: String, body: String, target: NotificationTarget): FcmNotification =
    FcmNotification(title, body, target)
}
