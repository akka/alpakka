/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.google.firebase.fcm.v1.models

/**
 * ApnsConfig model.
 * @see https://firebase.google.com/docs/reference/fcm/rest/v1/projects.messages#ApnsConfig
 */
case class ApnsConfig(
    headers: Option[Map[String, String]] = None,
    payload: Option[String] = None,
    fcm_options: Option[FcmOption] = None
) {
  def withHeaders(value: Map[String, String]): ApnsConfig = this.copy(headers = Option(value))
  def withPayload(value: String): ApnsConfig = this.copy(payload = Option(value))
  def withFcmOptions(value: FcmOption): ApnsConfig = this.copy(fcm_options = Option(value))
}
object ApnsConfig {
  val empty: ApnsConfig = ApnsConfig()
  def fromJava(): ApnsConfig = empty
}
