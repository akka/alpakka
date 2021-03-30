/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.huawei.pushkit.models

/**
 * ApnsConfig model.
 * @see https://developer.huawei.com/consumer/en/doc/development/HMSCore-References-V5/https-send-api-0000001050986197-V5#EN-US_TOPIC_0000001134031085
 */
case class ApnsConfig(hms_options: Option[String] = None,
                      headers: Option[String] = None,
                      payload: Option[String] = None) {
  def withHmsOptions(value: String): ApnsConfig = this.copy(hms_options = Option(value))
  def withHeaders(value: String): ApnsConfig = this.copy(headers = Option(value))
  def withPayload(value: String): ApnsConfig = this.copy(payload = Option(value))
}

object ApnsConfig {
  val empty: ApnsConfig = ApnsConfig()
  def fromJava(): ApnsConfig = empty
}
