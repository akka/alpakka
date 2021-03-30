/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.huawei.pushkit.models

/**
 * Notification model.
 * @see https://developer.huawei.com/consumer/en/doc/development/HMSCore-References-V5/https-send-api-0000001050986197-V5#EN-US_TOPIC_0000001134031085
 */
case class BasicNotification(title: Option[String] = None, body: Option[String] = None, image: Option[String] = None) {
  def withTitle(value: String): BasicNotification = this.copy(title = Option(value))
  def withBody(value: String): BasicNotification = this.copy(body = Option(value))
  def withImage(value: String): BasicNotification = this.copy(image = Option(value))
}

object BasicNotification {
  val empty: BasicNotification = BasicNotification()
  def fromJava(): BasicNotification = empty
}
