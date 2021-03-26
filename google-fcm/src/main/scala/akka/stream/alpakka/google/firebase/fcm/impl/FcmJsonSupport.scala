/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.google.firebase.fcm.impl

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import akka.stream.alpakka.google.firebase.fcm.{FcmErrorResponse, FcmResponse, FcmSuccessResponse}
import akka.annotation.InternalApi
import akka.stream.alpakka.google.firebase.fcm.FcmNotification
import akka.stream.alpakka.google.firebase.fcm.FcmNotificationModels._
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

  implicit object ApnsConfigResponseJsonFormat extends RootJsonFormat[ApnsConfig] {
    def write(c: ApnsConfig): JsObject =
      JsObject(
        "headers" -> c.headers.toJson,
        "payload" -> c.rawPayload.parseJson
      )

    def read(value: JsValue): ApnsConfig = {
      val map = value.asJsObject
      ApnsConfig(map.fields("headers").convertTo[Map[String, String]], map.fields("payload").toString)
    }
  }

  //app -> google
  implicit val webPushNotificationJsonFormat: RootJsonFormat[WebPushNotification] = jsonFormat3(WebPushNotification)
  implicit val webPushConfigJsonFormat: RootJsonFormat[WebPushConfig] = jsonFormat3(WebPushConfig.apply)
  implicit val androidNotificationJsonFormat: RootJsonFormat[AndroidNotification] = jsonFormat11(AndroidNotification)
  implicit val androidConfigJsonFormat: RootJsonFormat[AndroidConfig] = jsonFormat6(AndroidConfig.apply)
  implicit val basicNotificationJsonFormat: RootJsonFormat[BasicNotification] = jsonFormat2(BasicNotification)
  implicit val sendableFcmNotificationJsonFormat: RootJsonFormat[FcmNotification] = jsonFormat8(FcmNotification.apply)
  implicit val fcmSendJsonFormat: RootJsonFormat[FcmSend] = jsonFormat2(FcmSend)
}
