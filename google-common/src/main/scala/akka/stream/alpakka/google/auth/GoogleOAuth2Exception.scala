/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.google.auth

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import akka.http.scaladsl.model.{ErrorInfo, ExceptionWithErrorInfo, HttpResponse}
import akka.http.scaladsl.unmarshalling.{FromResponseUnmarshaller, PredefinedFromEntityUnmarshallers, Unmarshaller}
import akka.stream.alpakka.google.implicits._
import akka.stream.alpakka.google.util.Retry
import spray.json.DefaultJsonProtocol._
import spray.json.RootJsonFormat

final case class GoogleOAuth2Exception private (override val info: ErrorInfo) extends ExceptionWithErrorInfo(info)

private[google] object GoogleOAuth2Exception {

  private val internalFailure = "internal_failure"
  private final case class OAuth2ErrorResponse(error: Option[String], error_description: Option[String])
  private implicit val oAuth2ErrorResponseFormat: RootJsonFormat[OAuth2ErrorResponse] = jsonFormat2(OAuth2ErrorResponse)

  implicit val unmarshaller: FromResponseUnmarshaller[Throwable] =
    Unmarshaller
      .identityUnmarshaller[HttpResponse]
      .map(_.entity)
      .andThen(
        Unmarshaller.firstOf(
          sprayJsonUnmarshaller[OAuth2ErrorResponse],
          PredefinedFromEntityUnmarshallers.stringUnmarshaller.map(s => OAuth2ErrorResponse(None, Some(s)))
        )
      )
      .mapWithInput {
        case (response, OAuth2ErrorResponse(error, error_description)) =>
          val ex = GoogleOAuth2Exception(
            ErrorInfo(error.getOrElse(response.status.value),
                      error_description.getOrElse(response.status.defaultMessage))
          )
          // https://github.com/googleapis/google-auth-library-python/blob/master/google/oauth2/_client.py
          if (ex.info.summary == internalFailure || ex.info.detail == internalFailure)
            Retry(ex): Throwable
          else
            ex
      }
      .withDefaultRetry
}
