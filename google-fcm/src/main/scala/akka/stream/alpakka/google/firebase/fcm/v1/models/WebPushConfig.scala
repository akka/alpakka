/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.google.firebase.fcm.v1.models

/**
 * WebpushConfig model.
 * @see https://firebase.google.com/docs/reference/fcm/rest/v1/projects.messages#WebpushConfig
 */
case class WebPushConfig(headers: Option[Map[String, String]] = None,
                         data: Option[Map[String, String]] = None,
                         notification: Option[String] = None,
                         fcm_options: Option[FcmOption] = None
) {
  def withHeaders(value: Map[String, String]): WebPushConfig = this.copy(headers = Option(value))
  def withData(value: Map[String, String]): WebPushConfig = this.copy(data = Option(value))
  def withNotification(value: String): WebPushConfig = this.copy(notification = Option(value))
  def withFcmOptions(value: FcmOption): WebPushConfig = this.copy(fcm_options = Option(value))
}

object WebPushConfig {
  val empty: WebPushConfig = WebPushConfig()
  def fromJava(): WebPushConfig = empty
}
