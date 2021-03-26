/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.bigquery

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import akka.http.scaladsl.model.StatusCodes.Forbidden
import akka.http.scaladsl.model.{ErrorInfo, ExceptionWithErrorInfo, HttpResponse}
import akka.http.scaladsl.unmarshalling.{FromResponseUnmarshaller, PredefinedFromEntityUnmarshallers, Unmarshaller}
import akka.stream.alpakka.google.implicits._
import akka.stream.alpakka.google.util.Retry
import akka.stream.alpakka.googlecloud.bigquery.model.ErrorProto
import spray.json.DefaultJsonProtocol._
import spray.json.{enrichAny, RootJsonFormat}

final case class BigQueryException private (override val info: ErrorInfo, raw: String)
    extends ExceptionWithErrorInfo(info) {
  def getInfo = info
  def getRaw = raw
}

object BigQueryException {

  private[bigquery] def apply(message: String): BigQueryException =
    BigQueryException(ErrorInfo(message), message)

  implicit val fromResponseUnmarshaller: FromResponseUnmarshaller[Throwable] =
    Unmarshaller
      .withMaterializer { implicit ec => implicit mat => response: HttpResponse =>
        import SprayJsonSupport._
        val HttpResponse(status, _, entity, _) = response
        Unmarshaller
          .firstOf(
            sprayJsValueUnmarshaller.map { json =>
              json
                .convertTo[ErrorResponse]
                .error
                .fold[Throwable](BigQueryException(ErrorInfo(status.value, status.defaultMessage), json.prettyPrint)) {
                  case ErrorProto(reason, _, message) =>
                    val summary = reason.fold(status.value)(reason => s"${response.status.intValue} $reason")
                    val detail = message.getOrElse("")
                    BigQueryException(ErrorInfo(summary, detail), json.prettyPrint)
                }
            },
            PredefinedFromEntityUnmarshallers.stringUnmarshaller.map { error =>
              BigQueryException(ErrorInfo(status.value, error), error): Throwable
            }
          )
          .apply(entity)
      }
      .withDefaultRetry
      .mapWithInput {
        case (HttpResponse(Forbidden, _, _, _), ex @ BigQueryException(ErrorInfo(summary, _), _))
            if summary.contains("rateLimitExceeded") =>
          Retry(ex)
        case (_, ex) => ex
      }

  private[bigquery] def apply(error: ErrorProto): BigQueryException = error match {
    case ErrorProto(reason, location, message) =>
      val at = location.fold("")(loc => s" at $loc")
      val summary = reason.fold("")(reason => s"$reason$at")
      val info = ErrorInfo(summary, message.getOrElse(""))
      BigQueryException(info, error.toJson.prettyPrint)
  }

  private final case class ErrorResponse(error: Option[ErrorProto])
  private implicit val errorResponseFormat: RootJsonFormat[ErrorResponse] = jsonFormat1(ErrorResponse)
}
