/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.huawei.pushkit.impl

import akka.annotation.InternalApi
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import akka.stream.alpakka.huawei.pushkit.models.{ErrorResponse, PushKitResponse, Response}
import akka.stream.alpakka.huawei.pushkit.models._
import akka.stream.alpakka.huawei.pushkit.impl.HmsTokenApi.OAuthResponse
import spray.json._

/**
 * INTERNAL API
 */
@InternalApi
private[pushkit] case class PushKitSend(validate_only: Boolean, message: PushKitNotification)

/**
 * INTERNAL API
 */
@InternalApi
private[pushkit] object PushKitJsonSupport extends DefaultJsonProtocol with SprayJsonSupport {

  //custom formatters
  implicit object OAuthResponseJsonFormat extends RootJsonFormat[OAuthResponse] {
    override def write(c: OAuthResponse): JsValue = c.toJson(this)
    override def read(value: JsValue): OAuthResponse = value match {
      case JsObject(fields) if fields.contains("access_token") =>
        OAuthResponse(fields("access_token").convertTo[String],
                      fields("token_type").convertTo[String],
                      fields("expires_in").convertTo[Int])
      case other => throw DeserializationException(s"object containing `access_token` expected, but we get $other")
    }
  }

  implicit object HmsResponseJsonFormat extends RootJsonFormat[PushKitResponse] {
    def write(c: PushKitResponse): JsValue = c.toJson(this)

    def read(value: JsValue) = value match {
      case JsObject(fields) if fields.contains("code") && fields.contains("msg") =>
        PushKitResponse(
          requestId = if (fields.contains("requestId")) fields("requestId").convertTo[String] else null,
          code = fields("code").convertTo[String],
          msg = fields("msg").convertTo[String]
        )
      case other => throw DeserializationException(s"object containing `code`, `msg` expected, but we get $other")
    }
  }

  implicit object ErrorResponseJsonFormat extends RootJsonFormat[ErrorResponse] {
    def write(c: ErrorResponse): JsValue = c.rawError.parseJson
    def read(value: JsValue) = ErrorResponse(value.toString)
  }

  implicit object ResponseFormat extends RootJsonReader[Response] {
    def read(value: JsValue): Response = value match {
      case JsObject(fields) if fields.keys.exists(_ == "code") => value.convertTo[PushKitResponse]
      case JsObject(fields) if fields.keys.exists(_ != "code") => value.convertTo[ErrorResponse]
      case other => throw DeserializationException(s"Response expected, but we get $other")
    }
  }

  //android -> huawei push kit
  implicit object AndroidNotificationJsonFormat extends RootJsonFormat[AndroidNotification] {
    override def write(obj: AndroidNotification): JsObject = {
      val fields = scala.collection.mutable.Map[String, JsValue]()
      if (obj.title.isDefined) fields += "title" -> obj.title.get.toJson
      if (obj.body.isDefined) fields += "body" -> obj.body.get.toJson
      if (obj.icon.isDefined) fields += "icon" -> obj.icon.get.toJson
      if (obj.color.isDefined) fields += "color" -> obj.color.get.toJson
      if (obj.sound.isDefined) fields += "sound" -> obj.sound.get.toJson
      if (obj.default_sound.isDefined) fields += "default_sound" -> obj.default_sound.get.toJson
      if (obj.tag.isDefined) fields += "tag" -> obj.tag.get.toJson
      if (obj.click_action.isDefined) fields += "click_action" -> obj.click_action.get.toJson
      if (obj.body_loc_key.isDefined) fields += "body_loc_key" -> obj.body_loc_key.get.toJson
      if (obj.body_loc_args.isDefined) fields += "body_loc_args" -> obj.body_loc_args.get.toJson
      if (obj.title_loc_key.isDefined) fields += "title_loc_key" -> obj.title_loc_key.get.toJson
      if (obj.title_loc_args.isDefined) fields += "title_loc_args" -> obj.title_loc_args.get.toJson
      if (obj.multi_lang_key.isDefined) fields += "multi_lang_key" -> obj.multi_lang_key.get.parseJson
      if (obj.channel_id.isDefined) fields += "channel_id" -> obj.channel_id.get.toJson
      if (obj.notify_summary.isDefined) fields += "notify_summary" -> obj.notify_summary.get.toJson
      if (obj.image.isDefined) fields += "image" -> obj.image.get.toJson
      if (obj.style.isDefined) fields += "style" -> obj.style.get.toJson
      if (obj.big_title.isDefined) fields += "big_title" -> obj.big_title.get.toJson
      if (obj.big_body.isDefined) fields += "big_body" -> obj.big_body.get.toJson
      if (obj.auto_clear.isDefined) fields += "auto_clear" -> obj.auto_clear.get.toJson
      if (obj.notify_id.isDefined) fields += "notify_id" -> obj.notify_id.get.toJson
      if (obj.group.isDefined) fields += "group" -> obj.group.get.toJson
      if (obj.badge.isDefined) fields += "badge" -> obj.badge.get.toJson
      if (obj.ticker.isDefined) fields += "ticker" -> obj.ticker.get.toJson
      if (obj.when.isDefined) fields += "when" -> obj.when.get.toJson
      if (obj.importance.isDefined) fields += "importance" -> obj.importance.get.toJson
      if (obj.use_default_vibrate.isDefined) fields += "use_default_vibrate" -> obj.use_default_vibrate.get.toJson
      if (obj.use_default_light.isDefined) fields += "use_default_light" -> obj.use_default_light.get.toJson
      if (obj.vibrate_config.isDefined) fields += "vibrate_config" -> obj.vibrate_config.get.toJson
      if (obj.visibility.isDefined) fields += "visibility" -> obj.visibility.get.toJson
      if (obj.light_settings.isDefined) fields += "light_settings" -> obj.light_settings.get.toJson
      if (obj.foreground_show.isDefined) fields += "foreground_show" -> obj.foreground_show.get.toJson
      if (obj.profile_id.isDefined) fields += "profile_id" -> obj.profile_id.get.toJson
      if (obj.inbox_content.isDefined) fields += "inbox_content" -> obj.inbox_content.get.toJson
      if (obj.buttons.isDefined) fields += "buttons" -> obj.buttons.get.toJson
      JsObject(fields.toMap)
    }
    override def read(json: JsValue): AndroidNotification = {
      val map = json.asJsObject
      AndroidNotification(
        title = if (map.fields.contains("title")) Option(map.fields("title").toString) else None,
        body = if (map.fields.contains("body")) Option(map.fields("body").toString) else None,
        icon = if (map.fields.contains("icon")) Option(map.fields("icon").toString) else None,
        color = if (map.fields.contains("color")) Option(map.fields("color").toString) else None,
        sound = if (map.fields.contains("sound")) Option(map.fields("sound").toString) else None,
        default_sound =
          if (map.fields.contains("default_sound")) Option(map.fields("default_sound").convertTo[Boolean]) else None,
        tag = if (map.fields.contains("tag")) Option(map.fields("tag").toString) else None,
        click_action =
          if (map.fields.contains("click_action")) Option(map.fields("click_action").convertTo[ClickAction]) else None,
        body_loc_key = if (map.fields.contains("body_loc_key")) Option(map.fields("body_loc_key").toString) else None,
        body_loc_args =
          if (map.fields.contains("body_loc_args")) Option(map.fields("body_loc_args").convertTo[Seq[String]])
          else None,
        title_loc_key =
          if (map.fields.contains("title_loc_key")) Option(map.fields("title_loc_key").toString) else None,
        title_loc_args =
          if (map.fields.contains("title_loc_args")) Option(map.fields("title_loc_args").convertTo[Seq[String]])
          else None,
        multi_lang_key =
          if (map.fields.contains("multi_lang_key")) Option(map.fields("multi_lang_key").toString) else None,
        channel_id = if (map.fields.contains("channel_id")) Option(map.fields("channel_id").toString) else None,
        notify_summary =
          if (map.fields.contains("notify_summary")) Option(map.fields("notify_summary").toString) else None,
        image = if (map.fields.contains("image")) Option(map.fields("image").toString) else None,
        style = if (map.fields.contains("style")) Option(map.fields("style").convertTo[Int]) else None,
        big_title = if (map.fields.contains("big_title")) Option(map.fields("big_title").toString()) else None,
        big_body = if (map.fields.contains("big_body")) Option(map.fields("big_body").toString()) else None,
        auto_clear = if (map.fields.contains("auto_clear")) Option(map.fields("auto_clear").convertTo[Int]) else None,
        notify_id = if (map.fields.contains("notify_id")) Option(map.fields("notify_id").convertTo[Int]) else None,
        group = if (map.fields.contains("group")) Option(map.fields("group").toString()) else None,
        badge = if (map.fields.contains("badge")) Option(map.fields("badge").convertTo[BadgeNotification]) else None,
        ticker = if (map.fields.contains("ticker")) Option(map.fields("ticker").toString()) else None,
        when = if (map.fields.contains("when")) Option(map.fields("when").toString()) else None,
        importance = if (map.fields.contains("importance")) Option(map.fields("importance").toString()) else None,
        use_default_vibrate =
          if (map.fields.contains("use_default_vibrate")) Option(map.fields("use_default_vibrate").convertTo[Boolean])
          else None,
        use_default_light =
          if (map.fields.contains("use_default_light")) Option(map.fields("use_default_light").convertTo[Boolean])
          else None,
        vibrate_config =
          if (map.fields.contains("vibrate_config")) Option(map.fields("vibrate_config").convertTo[Seq[String]])
          else None,
        visibility = if (map.fields.contains("visibility")) Option(map.fields("visibility").toString()) else None,
        light_settings =
          if (map.fields.contains("light_settings")) Option(map.fields("light_settings").convertTo[LightSettings])
          else None,
        foreground_show =
          if (map.fields.contains("foreground_show")) Option(map.fields("foreground_show").convertTo[Boolean])
          else None,
        profile_id = if (map.fields.contains("profile_id")) Option(map.fields("profile_id").toString()) else None,
        inbox_content =
          if (map.fields.contains("inbox_content")) Option(map.fields("inbox_content").convertTo[Seq[String]])
          else None,
        buttons = if (map.fields.contains("buttons")) Option(map.fields("buttons").convertTo[Seq[Button]]) else None
      )
    }
  }

  //apns -> huawei push kit
  implicit object ApnsConfigResponseJsonFormat extends RootJsonFormat[ApnsConfig] {
    def write(obj: ApnsConfig): JsObject = {
      val fields = scala.collection.mutable.Map[String, JsValue]()
      if (obj.hms_options.isDefined) fields += "hms_options" -> obj.hms_options.get.parseJson
      if (obj.headers.isDefined) fields += "headers" -> obj.headers.get.parseJson
      if (obj.payload.isDefined) fields += "payload" -> obj.payload.get.parseJson
      JsObject(fields.toMap)
    }

    def read(json: JsValue): ApnsConfig = {
      val map = json.asJsObject
      ApnsConfig(
        hms_options = if (map.fields.contains("hms_options")) Option(map.fields("hms_options").toString) else None,
        headers = if (map.fields.contains("headers")) Option(map.fields("headers").toString) else None,
        payload = if (map.fields.contains("payload")) Option(map.fields("payload").toString) else None
      )
    }
  }

  //web -> huawei push kit
  implicit object WebPushConfigJsonFormat extends RootJsonFormat[WebConfig] {
    override def write(obj: WebConfig): JsObject = {
      val fields = scala.collection.mutable.Map[String, JsValue]()
      if (obj.hms_options.isDefined) fields += "hms_options" -> obj.hms_options.get.parseJson
      if (obj.headers.isDefined) fields += "headers" -> obj.headers.get.toJson
      if (obj.notification.isDefined) fields += "notification" -> obj.notification.get.toJson
      JsObject(fields.toMap)
    }

    override def read(json: JsValue): WebConfig = {
      val map = json.asJsObject
      WebConfig(
        hms_options = if (map.fields.contains("hms_options")) Option(map.fields("hms_options").toString()) else None,
        headers =
          if (map.fields.contains("headers")) Option(map.fields("headers").convertTo[Map[String, String]]) else None,
        notification =
          if (map.fields.contains("notification")) Option(map.fields("notification").convertTo[WebNotification])
          else None
      )
    }
  }

  //app -> huawei push kit
  implicit val androidConfigJsonFormat: RootJsonFormat[AndroidConfig] = jsonFormat7(AndroidConfig.apply)
  implicit val clickActionJsonFormat: RootJsonFormat[ClickAction] = jsonFormat4(ClickAction.apply)
  implicit val badgeNotificationJsonFormat: RootJsonFormat[BadgeNotification] = jsonFormat3(BadgeNotification.apply)
  implicit val buttonJsonFormat: RootJsonFormat[Button] = jsonFormat5(Button.apply)
  implicit val colorJsonFormat: RootJsonFormat[Color] = jsonFormat4(Color.apply)
  implicit val lightSettingsJsonFormat: RootJsonFormat[LightSettings] = jsonFormat3(LightSettings.apply)
  implicit val basicNotificationJsonFormat: RootJsonFormat[BasicNotification] = jsonFormat3(BasicNotification.apply)
  implicit val webActionsJsonFormat: RootJsonFormat[WebActions] = jsonFormat3(WebActions.apply)
  implicit val webNotificationJsonFormat: RootJsonFormat[WebNotification] = jsonFormat14(WebNotification.apply)
  implicit val pushKitNotificationJsonFormat: RootJsonFormat[PushKitNotification] = jsonFormat8(
    PushKitNotification.apply
  )
  implicit val pushKitSendJsonFormat: RootJsonFormat[PushKitSend] = jsonFormat2(PushKitSend)
}
