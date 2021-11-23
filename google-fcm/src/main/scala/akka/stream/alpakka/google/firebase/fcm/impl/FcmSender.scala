/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
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
import com.github.ghik.silencer.silent

import scala.concurrent.Future

/**
 * INTERNAL API
 */
@silent("deprecated")
@InternalApi
private[fcm] class FcmSender {
  import FcmJsonSupport._

  /** Use akka.stream.alpakka.google.firebase.fcm.v1.impl.FcmSender */
  @deprecated("Use akka.stream.alpakka.google.firebase.fcm.v1.impl.FcmSender", "3.0.2")
  @Deprecated
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
      case FcmErrorException(error) => error
    }
  }

  implicit private val unmarshaller: FromResponseUnmarshaller[FcmSuccessResponse] = Unmarshaller.withMaterializer {
    implicit ec => implicit mat => response: HttpResponse =>
      if (response.status.isSuccess) {
        Unmarshal(response.entity).to[FcmSuccessResponse]
      } else {
        Unmarshal(response.entity).to[FcmErrorResponse].map(error => throw FcmErrorException(error))
      }
  }.withDefaultRetry

  /** Use akka.stream.alpakka.google.firebase.fcm.v1.impl.FcmErrorException */
  @deprecated("Use akka.stream.alpakka.google.firebase.fcm.v1.impl.FcmErrorException", "3.0.2")
  @Deprecated
  private case class FcmErrorException(error: FcmErrorResponse) extends Exception
}
