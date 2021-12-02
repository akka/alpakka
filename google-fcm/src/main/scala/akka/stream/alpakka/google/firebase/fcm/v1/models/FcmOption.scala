/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.google.firebase.fcm.v1.models

sealed trait FcmOption

/**
 * FcmOptions model.
 * @see https://firebase.google.com/docs/reference/fcm/rest/v1/projects.messages#FcmOptions
 */
case class FcmOptions(analytics_label: String) extends FcmOption

object FcmOptions {
  def create(value: String): FcmOptions = FcmOptions(value)
}

/**
 * ApnsFcmOptions model.
 * @see https://firebase.google.com/docs/reference/fcm/rest/v1/projects.messages#ApnsFcmOptions
 */
case class ApnsFcmOptions(analytics_label: Option[String] = None, image: Option[String] = None) extends FcmOption {
  def withAnalyticsLabel(value: String): ApnsFcmOptions = this.copy(analytics_label = Option(value))
  def withImage(value: String): ApnsFcmOptions = this.copy(image = Option(value))
}
object ApnsFcmOptions {
  val empty: ApnsFcmOptions = ApnsFcmOptions()
  def fromJava(): ApnsFcmOptions = empty
}

/**
 * WebpushFcmOptions model.
 * @see https://firebase.google.com/docs/reference/fcm/rest/v1/projects.messages#WebpushFcmOptions
 */
case class WebPushFcmOptions(analytics_label: Option[String] = None, link: Option[String] = None) extends FcmOption {
  def withAnalyticsLabel(value: String): WebPushFcmOptions = this.copy(analytics_label = Option(value))
  def withLink(value: String): WebPushFcmOptions = this.copy(link = Option(value))
}
object WebPushFcmOptions {
  val empty: WebPushFcmOptions = WebPushFcmOptions()
  def fromJava(): WebPushFcmOptions = empty
}
