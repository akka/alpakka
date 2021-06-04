/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.google.firebase.fcm.v1.models

/**
 * Notification model.
 * @see https://firebase.google.com/docs/reference/fcm/rest/v1/projects.messages#Notification
 */
case class BasicNotification(title: String, body: String, image: Option[String] = None) {
  def withTitle(value: String): BasicNotification = this.copy(title = value)
  def withBody(value: String): BasicNotification = this.copy(body = value)
  def withImage(value: String): BasicNotification = this.copy(image = Option(value))
}

object BasicNotification {
  def create(title: String, body: String): BasicNotification =
    BasicNotification(title = title, body = body, image = None)
  def create(title: String, body: String, image: String): BasicNotification =
    BasicNotification(title = title, body = body, image = Option(image))
}
