/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.google.firebase.fcm.v1.impl

import akka.annotation.InternalApi
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import akka.stream.alpakka.google.firebase.fcm.v1.models._
import spray.json._

/**
 * INTERNAL API
 */
@InternalApi
private[fcm] case class FcmSend(validate_only: Boolean, message: FcmNotification)

/**
 * INTERNAL API
 */
@InternalApi
private[fcm] object FcmJsonSupport extends DefaultJsonProtocol with SprayJsonSupport {

  //custom formatters
  implicit object FcmSuccessResponseJsonFormat extends RootJsonFormat[FcmSuccessResponse] {
    def write(c: FcmSuccessResponse): JsValue = JsString(c.name)

    def read(value: JsValue) = value match {
      case JsObject(fields) if fields.contains("name") => FcmSuccessResponse(fields("name").convertTo[String])
      case other => throw DeserializationException(s"object containing `name` expected, but we get $other")
    }
  }
  implicit object FcmErrorResponseJsonFormat extends RootJsonFormat[FcmErrorResponse] {
    def write(c: FcmErrorResponse): JsValue = c.rawError.parseJson
    def read(value: JsValue) = FcmErrorResponse(value.toString)
  }

  implicit object FcmResponseFormat extends RootJsonReader[FcmResponse] {
    def read(value: JsValue): FcmResponse = value match {
      case JsObject(fields) if fields.keys.exists(_ == "name") => value.convertTo[FcmSuccessResponse]
      case JsObject(fields) if fields.keys.exists(_ == "error_code") => value.convertTo[FcmErrorResponse]
      case other => throw DeserializationException(s"FcmResponse expected, but we get $other")
    }
  }

  implicit object FcmOptionsFormat extends RootJsonFormat[FcmOption] {
    def write(c: FcmOption): JsValue = c match {
      case apns: ApnsFcmOptions => apns.toJson
      case webPush: WebPushFcmOptions => webPush.toJson
      case main: FcmOptions => main.toJson
    }
    def read(value: JsValue): FcmOption = value match {
      case JsObject(fields) if fields.contains("image") => value.convertTo[ApnsFcmOptions]
      case JsObject(fields) if fields.contains("link") => value.convertTo[WebPushFcmOptions]
      case JsObject(_) => value.convertTo[FcmOptions]
      case other: JsValue => throw DeserializationException(s"JsObject expected, but we get $other")
    }
  }

  implicit object ApnsConfigJsonFormat extends RootJsonFormat[ApnsConfig] {
    def write(c: ApnsConfig): JsObject = {
      val fields = scala.collection.mutable.Map[String, JsValue]()
      if (c.headers.isDefined) fields += "headers" -> c.headers.get.toJson
      if (c.payload.isDefined) fields += "payload" -> c.payload.get.parseJson
      if (c.fcm_options.isDefined) fields += "fcm_options" -> c.fcm_options.get.toJson
      JsObject(fields.toMap)
    }

    def read(value: JsValue): ApnsConfig = {
      val map = value.asJsObject
      ApnsConfig(
        headers =
          if (map.fields.contains("headers")) Option(map.fields("headers").convertTo[Map[String, String]]) else None,
        payload = if (map.fields.contains("payload")) Option(map.fields("payload").toString) else None,
        fcm_options =
          if (map.fields.contains("fcm_options")) Option(map.fields("fcm_options").convertTo[FcmOption]) else None
      )
    }
  }

  implicit object WebPushConfigJsonFormat extends RootJsonFormat[WebPushConfig] {
    def write(c: WebPushConfig): JsObject = {
      val fields = scala.collection.mutable.Map[String, JsValue]()
      if (c.headers.isDefined) fields += "headers" -> c.headers.get.toJson
      if (c.data.isDefined) fields += "data" -> c.data.get.toJson
      if (c.notification.isDefined) fields += "notification" -> c.notification.get.parseJson
      if (c.fcm_options.isDefined) fields += "fcm_options" -> c.fcm_options.get.toJson
      JsObject(fields.toMap)
    }

    def read(value: JsValue): WebPushConfig = {
      val map = value.asJsObject
      WebPushConfig(
        headers =
          if (map.fields.contains("headers")) Option(map.fields("headers").convertTo[Map[String, String]]) else None,
        data = if (map.fields.contains("data")) Option(map.fields("data").convertTo[Map[String, String]]) else None,
        notification = if (map.fields.contains("notification")) Option(map.fields("notification").toString) else None,
        fcm_options =
          if (map.fields.contains("fcm_options")) Option(map.fields("fcm_options").convertTo[FcmOption]) else None
      )
    }
  }

  implicit object AndroidMessagePriorityFormat extends RootJsonFormat[AndroidMessagePriority] {
    def write(c: AndroidMessagePriority): JsString =
      c match {
        case Normal => JsString("NORMAL")
        case High => JsString("HIGH")
      }

    def read(value: JsValue): AndroidMessagePriority = value match {
      case JsString("NORMAL") => Normal
      case JsString("HIGH") => High
      case other => throw DeserializationException(s"AndroidMessagePriority expected, but we get $other")
    }
  }

  implicit object NotificationPriorityFormat extends RootJsonFormat[NotificationPriority] {
    def write(c: NotificationPriority): JsString =
      c match {
        case PriorityMin => JsString("PRIORITY_MIN")
        case PriorityLow => JsString("PRIORITY_LOW")
        case PriorityDefault => JsString("PRIORITY_DEFAULT")
        case PriorityHigh => JsString("PRIORITY_HIGH")
        case PriorityMax => JsString("PRIORITY_MAX")
      }

    def read(value: JsValue): NotificationPriority = value match {
      case JsString("PRIORITY_MIN") => PriorityMin
      case JsString("PRIORITY_LOW") => PriorityLow
      case JsString("PRIORITY_DEFAULT") => PriorityDefault
      case JsString("PRIORITY_HIGH") => PriorityHigh
      case JsString("PRIORITY_MAX") => PriorityMax
      case other => throw DeserializationException(s"NotificationPriority expected, but we get $other")
    }
  }

  implicit object VisibilityFormat extends RootJsonFormat[Visibility] {
    def write(c: Visibility): JsString =
      c match {
        case Private => JsString("PRIVATE")
        case Public => JsString("PUBLIC")
        case Secret => JsString("SECRET")
      }

    def read(value: JsValue): Visibility = value match {
      case JsString("PRIVATE") => Private
      case JsString("PUBLIC") => Public
      case JsString("SECRET") => Secret
      case other => throw DeserializationException(s"Visibility expected, but we get $other")
    }
  }

  implicit object AndroidNotificationJsonFormat extends RootJsonFormat[AndroidNotification] {
    override def write(obj: AndroidNotification): JsObject = {
      val fields = scala.collection.mutable.Map[String, JsValue]()
      if (obj.title.isDefined) fields += "title" -> obj.title.get.toJson
      if (obj.body.isDefined) fields += "body" -> obj.body.get.toJson
      if (obj.icon.isDefined) fields += "icon" -> obj.icon.get.toJson
      if (obj.color.isDefined) fields += "color" -> obj.color.get.toJson
      if (obj.sound.isDefined) fields += "sound" -> obj.sound.get.toJson
      if (obj.tag.isDefined) fields += "tag" -> obj.tag.get.toJson
      if (obj.click_action.isDefined) fields += "click_action" -> obj.click_action.get.toJson
      if (obj.body_loc_key.isDefined) fields += "body_loc_key" -> obj.body_loc_key.get.toJson
      if (obj.body_loc_args.isDefined) fields += "body_loc_args" -> obj.body_loc_args.get.toJson
      if (obj.title_loc_key.isDefined) fields += "title_loc_key" -> obj.title_loc_key.get.toJson
      if (obj.title_loc_args.isDefined) fields += "title_loc_args" -> obj.title_loc_args.get.toJson
      if (obj.channel_id.isDefined) fields += "channel_id" -> obj.channel_id.get.toJson
      if (obj.ticker.isDefined) fields += "ticker" -> obj.ticker.get.toJson
      if (obj.sticky.isDefined) fields += "sticky" -> obj.sticky.get.toJson
      if (obj.event_time.isDefined) fields += "event_time" -> obj.event_time.get.toJson
      if (obj.local_only.isDefined) fields += "local_only" -> obj.local_only.get.toJson
      if (obj.notification_priority.isDefined) fields += "notification_priority" -> obj.notification_priority.get.toJson
      if (obj.default_sound.isDefined) fields += "default_sound" -> obj.default_sound.get.toJson
      if (obj.default_vibrate_timings.isDefined)
        fields += "default_vibrate_timings" -> obj.default_vibrate_timings.get.toJson
      if (obj.default_light_settings.isDefined)
        fields += "default_light_settings" -> obj.default_light_settings.get.toJson
      if (obj.vibrate_timings.isDefined) fields += "vibrate_timings" -> obj.vibrate_timings.get.toJson
      if (obj.visibility.isDefined) fields += "visibility" -> obj.visibility.get.toJson
      if (obj.notification_count.isDefined) fields += "notification_count" -> obj.notification_count.get.toJson
      if (obj.light_settings.isDefined) fields += "light_settings" -> obj.light_settings.get.toJson
      if (obj.image.isDefined) fields += "image" -> obj.image.get.toJson
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
        tag = if (map.fields.contains("tag")) Option(map.fields("tag").toString()) else None,
        click_action = if (map.fields.contains("click_action")) Option(map.fields("click_action").toString) else None,
        body_loc_key = if (map.fields.contains("body_loc_key")) Option(map.fields("body_loc_key").toString) else None,
        body_loc_args =
          if (map.fields.contains("body_loc_args")) Option(map.fields("body_loc_args").convertTo[Seq[String]])
          else None,
        title_loc_key =
          if (map.fields.contains("title_loc_key")) Option(map.fields("title_loc_key").toString) else None,
        title_loc_args =
          if (map.fields.contains("title_loc_args")) Option(map.fields("title_loc_args").convertTo[Seq[String]])
          else None,
        channel_id = if (map.fields.contains("channel_id")) Option(map.fields("channel_id").toString) else None,
        ticker = if (map.fields.contains("ticker")) Option(map.fields("ticker").toString) else None,
        sticky = if (map.fields.contains("sticky")) Option(map.fields("sticky").convertTo[Boolean]) else None,
        event_time = if (map.fields.contains("event_time")) Option(map.fields("event_time").toString()) else None,
        local_only =
          if (map.fields.contains("local_only")) Option(map.fields("local_only").convertTo[Boolean]) else None,
        notification_priority =
          if (map.fields.contains("notification_priority"))
            Option(map.fields("notification_priority").convertTo[NotificationPriority])
          else None,
        default_sound =
          if (map.fields.contains("default_sound")) Option(map.fields("default_sound").toString()) else None,
        default_vibrate_timings =
          if (map.fields.contains("default_vibrate_timings"))
            Option(map.fields("default_vibrate_timings").convertTo[Boolean])
          else None,
        default_light_settings =
          if (map.fields.contains("default_light_settings"))
            Option(map.fields("default_light_settings").convertTo[Boolean])
          else None,
        vibrate_timings =
          if (map.fields.contains("vibrate_timings")) Option(map.fields("vibrate_timings").convertTo[Seq[String]])
          else None,
        visibility =
          if (map.fields.contains("visibility")) Option(map.fields("visibility").convertTo[Visibility]) else None,
        notification_count =
          if (map.fields.contains("notification_count")) Option(map.fields("notification_count").convertTo[Int])
          else None,
        light_settings =
          if (map.fields.contains("light_settings")) Option(map.fields("light_settings").convertTo[LightSettings])
          else None,
        image = if (map.fields.contains("image")) Option(map.fields("image").toString()) else None
      )
    }
  }

  implicit val fcmOptionsJsonFormat: RootJsonFormat[FcmOptions] = jsonFormat1(FcmOptions.apply)
  implicit val apnsFcmOptionsJsonFormat: RootJsonFormat[ApnsFcmOptions] = jsonFormat2(ApnsFcmOptions.apply)
  implicit val webPushFcmOptionsJsonFormat: RootJsonFormat[WebPushFcmOptions] = jsonFormat2(WebPushFcmOptions.apply)
  implicit val androidColorJsonFormat: RootJsonFormat[Color] = jsonFormat4(Color)
  implicit val androidLightSettingsJsonFormat: RootJsonFormat[LightSettings] = jsonFormat3(LightSettings.apply)
  implicit val androidConfigJsonFormat: RootJsonFormat[AndroidConfig] = jsonFormat8(AndroidConfig.apply)
  implicit val basicNotificationJsonFormat: RootJsonFormat[BasicNotification] = jsonFormat3(BasicNotification.apply)
  implicit val mainFcmNotificationJsonFormat: RootJsonFormat[FcmNotification] = jsonFormat9(FcmNotification.apply)
  implicit val fcmSendJsonFormat: RootJsonFormat[FcmSend] = jsonFormat2(FcmSend)
}
