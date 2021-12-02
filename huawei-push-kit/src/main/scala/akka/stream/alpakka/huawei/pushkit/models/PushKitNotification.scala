/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.huawei.pushkit.models

/**
 * Message model.
 * @see https://developer.huawei.com/consumer/en/doc/development/HMSCore-References-V5/https-send-api-0000001050986197-V5#EN-US_TOPIC_0000001134031085
 */
case class PushKitNotification(data: Option[String] = None,
                               notification: Option[BasicNotification] = None,
                               android: Option[AndroidConfig] = None,
                               apns: Option[ApnsConfig] = None,
                               webpush: Option[WebConfig] = None,
                               token: Option[Seq[String]] = None,
                               topic: Option[String] = None,
                               condition: Option[String] = None) {
  def withNotification(notification: BasicNotification): PushKitNotification =
    this.copy(notification = Option(notification))

  def withData(data: String): PushKitNotification = this.copy(data = Option(data))

  def withAndroidConfig(android: AndroidConfig): PushKitNotification = this.copy(android = Option(android))

  def withApnsConfig(apns: ApnsConfig): PushKitNotification = this.copy(apns = Option(apns))

  def withWebConfig(web: WebConfig): PushKitNotification = this.copy(webpush = Option(web))

  def withTarget(target: NotificationTarget): PushKitNotification = target match {
    case Tokens(t) => this.copy(token = Option(t), topic = None, condition = None)
    case Topic(t) => this.copy(token = None, topic = Option(t), condition = None)
    case Condition(t) => this.copy(token = None, topic = None, condition = Option(t))
  }
}

object PushKitNotification {
  val empty: PushKitNotification = PushKitNotification()
  def fromJava(): PushKitNotification = empty
}
