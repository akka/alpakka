/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.huawei.pushkit.models

/**
 * AndroidConfig model.
 * @see https://developer.huawei.com/consumer/en/doc/development/HMSCore-References-V5/https-send-api-0000001050986197-V5#EN-US_TOPIC_0000001134031085
 */
case class AndroidConfig(collapse_key: Option[Int] = None,
                         ttl: Option[String] = None,
                         bi_tag: Option[String] = None,
                         receipt_id: Option[String] = None,
                         fast_app_target: Option[Int] = None,
                         data: Option[String] = None,
                         notification: Option[AndroidNotification] = None
) {
  def withCollapseKey(value: Int): AndroidConfig = this.copy(collapse_key = Option(value))

  def withTtl(value: String): AndroidConfig = this.copy(ttl = Option(value))

  def withBiTag(value: String): AndroidConfig = this.copy(bi_tag = Option(value))

  def withReceiptId(value: String): AndroidConfig = this.copy(receipt_id = Option(value))

  def withFastAppTarget(value: Int): AndroidConfig = this.copy(fast_app_target = Option(value))

  def withData(value: String): AndroidConfig = this.copy(data = Option(value))

  def withNotification(notification: AndroidNotification): AndroidConfig =
    this.copy(notification = Option(notification))
}

object AndroidConfig {
  val empty: AndroidConfig = AndroidConfig()

  def fromJava(): AndroidConfig = empty
}

/**
 * AndroidNotification model.
 *
 * @see https://developer.huawei.com/consumer/en/doc/development/HMSCore-References-V5/https-send-api-0000001050986197-V5#EN-US_TOPIC_0000001134031085
 */
case class AndroidNotification(title: Option[String] = None,
                               body: Option[String] = None,
                               icon: Option[String] = None,
                               color: Option[String] = None,
                               sound: Option[String] = None,
                               default_sound: Option[Boolean] = None,
                               tag: Option[String] = None,
                               click_action: Option[ClickAction] = None,
                               body_loc_key: Option[String] = None,
                               body_loc_args: Option[Seq[String]] = None,
                               title_loc_key: Option[String] = None,
                               title_loc_args: Option[Seq[String]] = None,
                               multi_lang_key: Option[String] = None,
                               channel_id: Option[String] = None,
                               notify_summary: Option[String] = None,
                               image: Option[String] = None,
                               style: Option[Int] = None,
                               big_title: Option[String] = None,
                               big_body: Option[String] = None,
                               auto_clear: Option[Int] = None,
                               notify_id: Option[Int] = None,
                               group: Option[String] = None,
                               badge: Option[BadgeNotification] = None,
                               ticker: Option[String] = None,
                               when: Option[String] = None,
                               importance: Option[String] = None,
                               use_default_vibrate: Option[Boolean] = None,
                               use_default_light: Option[Boolean] = None,
                               vibrate_config: Option[Seq[String]] = None,
                               visibility: Option[String] = None,
                               light_settings: Option[LightSettings] = None,
                               foreground_show: Option[Boolean] = None,
                               profile_id: Option[String] = None,
                               inbox_content: Option[Seq[String]] = None,
                               buttons: Option[Seq[Button]] = None
) {
  def withTitle(value: String): AndroidNotification = this.copy(title = Option(value))
  def withBody(value: String): AndroidNotification = this.copy(body = Option(value))
  def withIcon(value: String): AndroidNotification = this.copy(icon = Option(value))
  def withColor(value: String): AndroidNotification = this.copy(color = Option(value))
  def withSound(value: String): AndroidNotification = this.copy(sound = Option(value))
  def withDefaultSound(value: Boolean): AndroidNotification = this.copy(default_sound = Option(value))
  def withTag(value: String): AndroidNotification = this.copy(tag = Option(value))
  def withClickAction(clickAction: ClickAction): AndroidNotification = this.copy(click_action = Option(clickAction))
  def withBodyLocKey(value: String): AndroidNotification = this.copy(body_loc_key = Option(value))
  def withBodyLocArgs(values: Seq[String]): AndroidNotification = this.copy(body_loc_args = Option(values))
  def withTitleLocKey(value: String): AndroidNotification = this.copy(title_loc_key = Option(value))
  def withTitleLocArgs(values: Seq[String]): AndroidNotification = this.copy(title_loc_args = Option(values))
  def withMultiLangKey(value: String): AndroidNotification = this.copy(multi_lang_key = Option(value))
  def withChannelId(value: String): AndroidNotification = this.copy(channel_id = Option(value))
  def withNotifySummary(value: String): AndroidNotification = this.copy(notify_summary = Option(value))
  def withImage(value: String): AndroidNotification = this.copy(image = Option(value))
  def withStyle(value: Int): AndroidNotification = this.copy(style = Option(value))
  def withBigTitle(value: String): AndroidNotification = this.copy(big_title = Option(value))
  def withBigBody(value: String): AndroidNotification = this.copy(big_body = Option(value))
  def withAutoClear(value: Int): AndroidNotification = this.copy(auto_clear = Option(value))
  def withNotifyId(value: Int): AndroidNotification = this.copy(notify_id = Option(value))
  def withGroup(value: String): AndroidNotification = this.copy(group = Option(value))
  def withBadge(notification: BadgeNotification): AndroidNotification = this.copy(badge = Option(notification))
  def withTicker(value: String): AndroidNotification = this.copy(ticker = Option(value))
  def withWhen(value: String): AndroidNotification = this.copy(when = Option(value))
  def withImportance(value: String): AndroidNotification = this.copy(importance = Option(value))
  def withUseDefaultVibrate(value: Boolean): AndroidNotification = this.copy(use_default_vibrate = Option(value))
  def withUseDefaultLight(value: Boolean): AndroidNotification = this.copy(use_default_light = Option(value))
  def withVibrateConfig(values: Seq[String]): AndroidNotification = this.copy(vibrate_config = Option(values))
  def withVisibility(value: String): AndroidNotification = this.copy(visibility = Option(value))
  def withLightSettings(settings: LightSettings): AndroidNotification = this.copy(light_settings = Option(settings))
  def withForegroundShow(value: Boolean): AndroidNotification = this.copy(foreground_show = Option(value))
  def withProfileId(value: String): AndroidNotification = this.copy(profile_id = Option(value))
  def withInboxContent(value: Seq[String]): AndroidNotification = this.copy(inbox_content = Option(value))
  def withButtons(buttons: Seq[Button]): AndroidNotification = this.copy(buttons = Option(buttons))
}

object AndroidNotification {
  val empty: AndroidNotification = AndroidNotification()

  def fromJava(): AndroidNotification = empty
}

/**
 * LightSettings model.
 */
case class LightSettings(color: Option[Color] = None,
                         light_on_duration: Option[String] = None,
                         light_off_duration: Option[String] = None
) {
  def withColor(color: Color): LightSettings = this.copy(color = Option(color))

  def withLightOnDuration(value: String): LightSettings = this.copy(light_on_duration = Option(value))

  def withLightOffDuration(value: String): LightSettings = this.copy(light_off_duration = Option(value))
}

object LightSettings {
  val empty: LightSettings = LightSettings()

  def fromJava(): LightSettings = empty
}

/**
 * Color model.
 */
case class Color(alpha: Option[Float] = None,
                 red: Option[Float] = None,
                 green: Option[Float] = None,
                 blue: Option[Float] = None
) {
  def withAlpha(value: Float): Color = this.copy(alpha = Option(value))

  def withRed(value: Float): Color = this.copy(red = Option(value))

  def withGreen(value: Float): Color = this.copy(green = Option(value))

  def withBlue(value: Float): Color = this.copy(blue = Option(value))
}

object Color {
  val empty: Color = Color()

  def fromJava(): Color = empty
}

/**
 * Click Action model.
 */
case class ClickAction(`type`: Option[Int] = None,
                       intent: Option[String] = None,
                       url: Option[String] = None,
                       action: Option[String] = None
) {
  def withType(value: Int): ClickAction = this.copy(`type` = Option(value))

  def withIntent(value: String): ClickAction = this.copy(intent = Option(value))

  def withUrl(value: String): ClickAction = this.copy(url = Option(value))

  def withAction(value: String): ClickAction = this.copy(action = Option(value))
}

object ClickAction {
  val empty: ClickAction = ClickAction()

  def fromJava(): ClickAction = empty
}

/**
 * BadgeNotification model.
 */
case class BadgeNotification(add_num: Option[Int] = None, `class`: Option[String] = None, set_num: Option[Int] = None) {
  def withAddNum(value: Int): BadgeNotification = this.copy(add_num = Option(value))
  def withClass(value: String): BadgeNotification = this.copy(`class` = Option(value))
  def withSetNum(value: Int): BadgeNotification = this.copy(set_num = Option(value))
}
object BadgeNotification {
  val empty: BadgeNotification = BadgeNotification()

  def fromJava(): BadgeNotification = empty
}

/**
 * Button model.
 */
case class Button(name: Option[String] = None,
                  action_type: Option[Int] = None,
                  intent_type: Option[Int] = None,
                  intent: Option[String] = None,
                  data: Option[String] = None
) {
  def withName(value: String): Button = this.copy(name = Option(value))
  def withActionType(value: Int): Button = this.copy(action_type = Option(value))
  def withIntentType(value: Int): Button = this.copy(intent_type = Option(value))
  def withIntent(value: String): Button = this.copy(intent = Option(value))
  def withData(value: String): Button = this.copy(data = Option(value))
}

object Button {
  val empty: Button = Button()
  def fromJava(): Button = empty
}
