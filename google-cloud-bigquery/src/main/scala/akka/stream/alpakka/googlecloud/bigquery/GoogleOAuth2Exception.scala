/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.bigquery

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import akka.http.scaladsl.model.{ErrorInfo, ExceptionWithErrorInfo}
import akka.http.scaladsl.unmarshalling.{FromResponseUnmarshaller, PredefinedFromEntityUnmarshallers, Unmarshaller}
import spray.json.DefaultJsonProtocol._
import spray.json.RootJsonFormat

final case class GoogleOAuth2Exception private (override val info: ErrorInfo) extends ExceptionWithErrorInfo(info)

private[bigquery] object GoogleOAuth2Exception {

  implicit val fromResponseUnmarshaller: FromResponseUnmarshaller[Exception] =
    Unmarshaller.withMaterializer { implicit ec => implicit mat => response =>
      Unmarshaller
        .firstOf(
          sprayJsonUnmarshaller[OAuth2ErrorResponse].map(_.error),
          PredefinedFromEntityUnmarshallers.stringUnmarshaller
        )
        .apply(response.entity)
        .map { detail =>
          GoogleOAuth2Exception(ErrorInfo(response.status.value, detail))
        }
    }

  private final case class OAuth2ErrorResponse(error: String)
  private implicit val oAuth2ErrorResponseFormat: RootJsonFormat[OAuth2ErrorResponse] = jsonFormat1(OAuth2ErrorResponse)
}
