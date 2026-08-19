/*
 * Copyright (C) since 2016 Lightbend Inc. <https://akka.io>
 */

package akka.stream.alpakka.google.auth

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import akka.http.scaladsl.model.{ErrorInfo, ExceptionWithErrorInfo, HttpResponse, Uri}
import akka.http.scaladsl.unmarshalling.{
  FromEntityUnmarshaller,
  FromResponseUnmarshaller,
  PredefinedFromEntityUnmarshallers,
  Unmarshal,
  Unmarshaller
}
import akka.stream.Materializer
import akka.stream.alpakka.google.implicits._
import akka.stream.alpakka.google.util.Retry
import spray.json.DefaultJsonProtocol._
import spray.json.RootJsonFormat

import scala.concurrent.{ExecutionContext, Future}

final case class GoogleOAuth2Exception private[akka] (override val info: ErrorInfo) extends ExceptionWithErrorInfo(info)

private[google] object GoogleOAuth2Exception {

  private val internalFailure = "internal_failure"
  private final case class OAuth2ErrorResponse(error: Option[String], error_description: Option[String])
  private implicit val oAuth2ErrorResponseFormat: RootJsonFormat[OAuth2ErrorResponse] = jsonFormat2(
    OAuth2ErrorResponse.apply
  )

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

  private val MaxErrorBodyLength = 512

  private def truncate(body: String) =
    if (body.length > MaxErrorBodyLength) body.take(MaxErrorBodyLength) + "..." else body

  /**
   * Unmarshals a successful response, or fails with a [[GoogleOAuth2Exception]] describing the status and body of an
   * unsuccessful one. Without the status check an error response is unmarshalled as if it were a token, which reports
   * an unrelated parsing failure (e.g. `Unexpected end-of-input` for an empty body) and hides both the status and any
   * explanation the endpoint gave. Intended for the credentials endpoints that are requested directly, without the
   * [[akka.stream.alpakka.google.http.GoogleHttp]] unmarshalling and retry machinery.
   */
  private[auth] def unmarshalOrFail[T](uri: Uri, response: HttpResponse)(
      implicit um: FromEntityUnmarshaller[T],
      ec: ExecutionContext,
      mat: Materializer
  ): Future[T] =
    if (response.status.isSuccess())
      Unmarshal(response.entity).to[T]
    else
      Unmarshal(response.entity)
        .to[String]
        .recover { case _ => "<unavailable>" }
        .flatMap { body =>
          Future.failed(
            GoogleOAuth2Exception(
              ErrorInfo(
                s"Unexpected ${response.status.value} response from [$uri]",
                s"Response body: [${truncate(body)}]"
              )
            )
          )
        }
}
