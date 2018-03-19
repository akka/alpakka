/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.google.firebase.fcm.impl

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import akka.stream.alpakka.google.firebase.fcm.FcmFlowModels.{
  FcmErrorResponse,
  FcmResponse,
  FcmSend,
  FcmSuccessResponse
}
import akka.stream.alpakka.google.firebase.fcm.FcmNotification
import akka.stream.alpakka.google.firebase.fcm.FcmNotificationModels._
import akka.stream.alpakka.google.firebase.fcm.impl.GoogleTokenApi.OAuthResponse
import spray.json._

private[fcm] trait FcmJsonSupport extends DefaultJsonProtocol with SprayJsonSupport {

  //custom formatters
  implicit val fcmSuccessResponseJsonFormat: RootJsonFormat[FcmSuccessResponse] = jsonFormat1(FcmSuccessResponse)
  implicit object FcmErrorResponseJsonFormat extends RootJsonFormat[FcmErrorResponse] {
    def write(c: FcmErrorResponse) =
      c.rawError.parseJson

    def read(value: JsValue) = FcmErrorResponse(value.toString)
  }

  implicit object FcmResponseFormat extends RootJsonReader[FcmResponse] {
    def read(value: JsValue) = value match {
      case JsObject(fields) if fields.keys.exists(_ == "name") => value.convertTo[FcmSuccessResponse]
      case JsObject(fields) if fields.keys.exists(_ == "error_code") => value.convertTo[FcmErrorResponse]
      case other => throw DeserializationException(s"FcmResponse expected, but we get $other")
    }
  }

  implicit object AndroidMessagePriorityFormat extends RootJsonFormat[AndroidMessagePriority] {
    def write(c: AndroidMessagePriority) =
      c match {
        case Normal => JsString("NORMAL")
        case High => JsString("HIGH")
      }

    def read(value: JsValue) = value match {
      case JsString("NORMAL") => Normal
      case JsString("HIGH") => High
      case other => throw DeserializationException(s"AndroidMessagePriority expected, but we get $other")
    }
  }

  implicit object ApnsConfigResponseJsonFormat extends RootJsonFormat[ApnsConfig] {
    def write(c: ApnsConfig) =
      JsObject(
        "headers" -> c.headers.toJson,
        "payload" -> c.rawPayload.parseJson
      )

    def read(value: JsValue) = {
      val map = value.asJsObject
      ApnsConfig(map.fields("headers").convertTo[Map[String, String]], map.fields("payload").toString)
    }
  }

  // google -> app
  implicit val oAuthResponseJsonFormat: RootJsonFormat[OAuthResponse] = jsonFormat3(OAuthResponse)
  //app -> google
  implicit val webPushNotificationJsonFormat: RootJsonFormat[WebPushNotification] = jsonFormat3(WebPushNotification)
  implicit val webPushConfigJsonFormat: RootJsonFormat[WebPushConfig] = jsonFormat3(WebPushConfig.apply _)
  implicit val androidNotificationJsonFormat: RootJsonFormat[AndroidNotification] = jsonFormat11(AndroidNotification)
  implicit val androidConfigJsonFormat: RootJsonFormat[AndroidConfig] = jsonFormat6(AndroidConfig.apply _)
  implicit val basicNotificationJsonFormat: RootJsonFormat[BasicNotification] = jsonFormat2(BasicNotification)
  implicit val sendableFcmNotificationJsonFormat: RootJsonFormat[FcmNotification] = jsonFormat8(
    FcmNotification.apply _
  )
  implicit val fcmSendJsonFormat: RootJsonFormat[FcmSend] = jsonFormat2(FcmSend)
}
