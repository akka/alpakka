/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.huawei.pushkit.models

/**
 * WebPushConfig model.
 * @see https://developer.huawei.com/consumer/en/doc/development/HMSCore-References-V5/https-send-api-0000001050986197-V5#EN-US_TOPIC_0000001134031085
 */
case class WebConfig(hms_options: Option[String] = None,
                     headers: Option[Map[String, String]] = None,
                     notification: Option[WebNotification] = None
) {
  def withHmsOptions(options: String): WebConfig = this.copy(hms_options = Option(options))
  def withHeaders(headers: Map[String, String]): WebConfig = this.copy(headers = Option(headers))
  def withNotification(notification: WebNotification): WebConfig =
    this.copy(notification = Option(notification))
}

object WebConfig {
  val empty: WebConfig = WebConfig()
  def fromJava(): WebConfig = empty
}

/**
 * WebNotification model.
 */
case class WebNotification(title: Option[String] = None,
                           body: Option[String] = None,
                           icon: Option[String] = None,
                           image: Option[String] = None,
                           lang: Option[String] = None,
                           tag: Option[String] = None,
                           badge: Option[String] = None,
                           dir: Option[String] = None,
                           vibrate: Option[Seq[Int]] = None,
                           renotify: Option[Boolean] = None,
                           require_interaction: Option[Boolean] = None,
                           silent: Option[Boolean] = None,
                           timestamp: Option[Long] = None,
                           actions: Option[Seq[WebActions]] = None
) {
  def withTitle(value: String): WebNotification = this.copy(title = Option(value))
  def withBody(value: String): WebNotification = this.copy(body = Option(value))
  def withIcon(value: String): WebNotification = this.copy(icon = Option(value))
  def withImage(value: String): WebNotification = this.copy(image = Option(value))
  def withLang(value: String): WebNotification = this.copy(lang = Option(value))
  def withTag(value: String): WebNotification = this.copy(tag = Option(value))
  def withBadge(value: String): WebNotification = this.copy(badge = Option(value))
  def withDir(value: String): WebNotification = this.copy(dir = Option(value))
  def withVibrate(value: Seq[Int]): WebNotification = this.copy(vibrate = Option(value))
  def withRenotify(value: Boolean): WebNotification = this.copy(renotify = Option(value))
  def withRequireInteraction(value: Boolean): WebNotification = this.copy(require_interaction = Option(value))
  def withSilent(value: Boolean): WebNotification = this.copy(silent = Option(value))
  def withTimestamp(value: Long): WebNotification = this.copy(timestamp = Option(value))
  def withActions(actions: Seq[WebActions]): WebNotification = this.copy(actions = Option(actions))
}

object WebNotification {
  val empty: WebNotification = WebNotification()
  def fromJava(): WebNotification = empty
}

/**
 * WebActions model.
 */
case class WebActions(action: Option[String] = None, icon: Option[String] = None, title: Option[String] = None) {
  def withAction(value: String): WebActions = this.copy(action = Option(value))
  def withIcon(value: String): WebActions = this.copy(icon = Option(value))
  def withTitle(value: String): WebActions = this.copy(title = Option(value))
}

object WebActions {
  val empty: WebActions = WebActions()
  def fromJava(): WebActions = empty
}
