/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.google.firebase.fcm.v1.models

/**
 * AndroidConfig model.
 * @see https://firebase.google.com/docs/reference/fcm/rest/v1/projects.messages#AndroidConfig
 */
case class AndroidConfig(
    collapse_key: Option[String] = None,
    priority: Option[AndroidMessagePriority] = None,
    ttl: Option[String] = None,
    restricted_package_name: Option[String] = None,
    data: Option[Map[String, String]] = None,
    notification: Option[AndroidNotification] = None,
    fcm_options: Option[FcmOption] = None,
    direct_boot_ok: Option[Boolean] = None
) {
  def withCollapseKey(value: String): AndroidConfig = this.copy(collapse_key = Option(value))

  def withPriority(value: AndroidMessagePriority): AndroidConfig = this.copy(priority = Option(value))

  def withTtl(value: String): AndroidConfig = this.copy(ttl = Option(value))

  def withRestrictedPackageName(value: String): AndroidConfig = this.copy(restricted_package_name = Option(value))

  def withData(value: Map[String, String]): AndroidConfig = this.copy(data = Option(value))

  def withNotification(value: AndroidNotification): AndroidConfig = this.copy(notification = Option(value))

  def withFcmOptions(value: FcmOption): AndroidConfig = this.copy(fcm_options = Option(value))

  def withDirectBootOk(value: Boolean): AndroidConfig = this.copy(direct_boot_ok = Option(value))
}

object AndroidConfig {
  val empty: AndroidConfig = AndroidConfig()

  def fromJava(): AndroidConfig = empty
}

/**
 * AndroidNotification model.
 * @see https://firebase.google.com/docs/reference/fcm/rest/v1/projects.messages#AndroidNotification
 */
case class AndroidNotification(
    title: Option[String] = None,
    body: Option[String] = None,
    icon: Option[String] = None,
    color: Option[String] = None,
    sound: Option[String] = None,
    tag: Option[String] = None,
    click_action: Option[String] = None,
    body_loc_key: Option[String] = None,
    body_loc_args: Option[Seq[String]] = None,
    title_loc_key: Option[String] = None,
    title_loc_args: Option[Seq[String]] = None,
    channel_id: Option[String] = None,
    ticker: Option[String] = None,
    sticky: Option[Boolean] = None,
    event_time: Option[String] = None,
    local_only: Option[Boolean] = None,
    notification_priority: Option[NotificationPriority] = None,
    default_sound: Option[String] = None,
    default_vibrate_timings: Option[Boolean] = None,
    default_light_settings: Option[Boolean] = None,
    vibrate_timings: Option[Seq[String]] = None,
    visibility: Option[Visibility] = None,
    notification_count: Option[Int] = None,
    light_settings: Option[LightSettings] = None,
    image: Option[String] = None
) {
  def withTitle(value: String): AndroidNotification = this.copy(title = Option(value))

  def withBody(value: String): AndroidNotification = this.copy(body = Option(value))

  def withIcon(value: String): AndroidNotification = this.copy(icon = Option(value))

  def withColor(value: String): AndroidNotification = this.copy(color = Option(value))

  def withSound(value: String): AndroidNotification = this.copy(sound = Option(value))

  def withTag(value: String): AndroidNotification = this.copy(tag = Option(value))

  def withClickAction(value: String): AndroidNotification = this.copy(click_action = Option(value))

  def withBodyLocKey(value: String): AndroidNotification = this.copy(body_loc_key = Option(value))

  def withBodyLocArgs(value: Seq[String]): AndroidNotification = this.copy(body_loc_args = Option(value))

  def withTitleLocKey(value: String): AndroidNotification = this.copy(title_loc_key = Option(value))

  def withTitleLocArgs(value: Seq[String]): AndroidNotification = this.copy(title_loc_args = Option(value))

  def withChannelId(value: String): AndroidNotification = this.copy(channel_id = Option(value))

  def withTicker(value: String): AndroidNotification = this.copy(ticker = Option(value))

  def withSticky(value: Boolean): AndroidNotification = this.copy(sticky = Option(value))

  def withEventTime(value: String): AndroidNotification = this.copy(event_time = Option(value))

  def withLocalOnly(value: Boolean): AndroidNotification = this.copy(local_only = Option(value))

  def withNotificationPriority(value: NotificationPriority): AndroidNotification =
    this.copy(notification_priority = Option(value))

  def withDefaultSound(value: String): AndroidNotification = this.copy(default_sound = Option(value))

  def withDefaultVibrateTimings(value: Boolean): AndroidNotification =
    this.copy(default_vibrate_timings = Option(value))

  def withDefaultLightSettings(value: Boolean): AndroidNotification =
    this.copy(default_light_settings = Option(value))

  def withVibrateTimings(value: Seq[String]): AndroidNotification = this.copy(vibrate_timings = Option(value))

  def withVisibility(value: Visibility): AndroidNotification = this.copy(visibility = Option(value))

  def withNotificationCount(value: Int): AndroidNotification = this.copy(notification_count = Option(value))

  def withLightSettings(value: LightSettings): AndroidNotification = this.copy(light_settings = Option(value))

  def withImage(value: String): AndroidNotification = this.copy(image = Option(value))
}

object AndroidNotification {
  val empty: AndroidNotification = AndroidNotification()

  def fromJava(): AndroidNotification = empty
}

/**
 * AndroidMessagePriority model.
 * @see https://firebase.google.com/docs/reference/fcm/rest/v1/projects.messages#AndroidMessagePriority
 */
sealed trait AndroidMessagePriority

case object Normal extends AndroidMessagePriority

case object High extends AndroidMessagePriority

/**
 * NotificationPriority model.
 * @see https://firebase.google.com/docs/reference/fcm/rest/v1/projects.messages#NotificationPriority
 */
sealed trait NotificationPriority

case object PriorityMin extends NotificationPriority

case object PriorityLow extends NotificationPriority

case object PriorityDefault extends NotificationPriority

case object PriorityHigh extends NotificationPriority

case object PriorityMax extends NotificationPriority

/**
 * Visibility model.
 * @see https://firebase.google.com/docs/reference/fcm/rest/v1/projects.messages#Visibility
 */
sealed trait Visibility

case object Private extends Visibility

case object Public extends Visibility

case object Secret extends Visibility

/**
 * LightSettings model.
 * @see https://firebase.google.com/docs/reference/fcm/rest/v1/projects.messages#LightSettings
 */
case class LightSettings(
    color: Option[Color] = None,
    light_on_duration: Option[String] = None,
    light_off_duration: Option[String] = None
) {
  def withColor(value: Color): LightSettings = this.copy(color = Option(value))

  def withLightOnDuration(value: String): LightSettings = this.copy(light_on_duration = Option(value))

  def light_off_duration(value: String): LightSettings = this.copy(light_on_duration = Option(value))
}

object LightSettings {
  val empty: LightSettings = LightSettings()

  def fromJava(): LightSettings = empty
}

/**
 * Color model.
 * @see https://firebase.google.com/docs/reference/fcm/rest/v1/projects.messages#Color
 */
case class Color(red: Double, green: Double, blue: Double, alpha: Double)
