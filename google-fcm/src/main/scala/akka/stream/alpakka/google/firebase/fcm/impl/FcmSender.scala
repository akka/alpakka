/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.google.firebase.fcm.impl

import akka.annotation.InternalApi
import akka.http.scaladsl.HttpExt
import akka.http.scaladsl.marshalling.Marshal
import akka.http.scaladsl.model._
import akka.http.scaladsl.unmarshalling.{FromResponseUnmarshaller, Unmarshal, Unmarshaller}
import akka.stream.Materializer
import akka.stream.alpakka.google.GoogleSettings
import akka.stream.alpakka.google.firebase.fcm.{FcmErrorResponse, FcmResponse, FcmSuccessResponse}
import akka.stream.alpakka.google.http.GoogleHttp
import akka.stream.alpakka.google.implicits._

import scala.concurrent.Future

/**
 * INTERNAL API
 */
@InternalApi
private[fcm] class FcmSender {
  import FcmJsonSupport._

  def send(http: HttpExt, fcmSend: FcmSend)(
      implicit mat: Materializer,
      settings: GoogleSettings
  ): Future[FcmResponse] = {
    import mat.executionContext
    import settings.projectId
    val url = s"https://fcm.googleapis.com/v1/projects/$projectId/messages:send"

    Marshal(fcmSend).to[RequestEntity].flatMap { entity =>
      GoogleHttp(http)
        .singleAuthenticatedRequest[FcmSuccessResponse](HttpRequest(HttpMethods.POST, url, entity = entity))
    } recover {
      case error: FcmErrorResponse => error
    }
  }

  implicit private val unmarshaller: FromResponseUnmarshaller[FcmSuccessResponse] = Unmarshaller.withMaterializer {
    implicit ec => implicit mat => response: HttpResponse =>
      if (response.status.isSuccess) {
        Unmarshal(response.entity).to[FcmSuccessResponse]
      } else {
        Unmarshal(response.entity).to[FcmErrorResponse].map(throw _)
      }
  }.withDefaultRetry
}
