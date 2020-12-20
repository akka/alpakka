/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.bigquery

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import akka.http.scaladsl.model.{ErrorInfo, ExceptionWithErrorInfo}
import akka.http.scaladsl.unmarshalling.{FromResponseUnmarshaller, PredefinedFromEntityUnmarshallers, Unmarshaller}
import akka.stream.alpakka.googlecloud.bigquery.model.ErrorProtoJsonProtocol.ErrorProto

final case class BigQueryException private (_info: ErrorInfo) extends ExceptionWithErrorInfo(_info)

object BigQueryException {

  private[bigquery] def apply(message: String): BigQueryException =
    BigQueryException(ErrorInfo(message))

  implicit val fromResponseUnmarshaller: FromResponseUnmarshaller[Exception] =
    Unmarshaller.withMaterializer { implicit ec => implicit mat => response =>
      import SprayJsonSupport._
      Unmarshaller
        .firstOf(
          sprayJsValueUnmarshaller.map(_.prettyPrint),
          PredefinedFromEntityUnmarshallers.stringUnmarshaller
        )
        .apply(response.entity)
        .map { detail =>
          BigQueryException(ErrorInfo(response.status.value, detail))
        }
    }

  private[bigquery] def apply(error: ErrorProto): BigQueryException = error match {
    case ErrorProto(reason, location, message) =>
      val at = location.fold("")(loc => s" at $loc")
      BigQueryException(ErrorInfo(s"$reason$at", message))
  }
}
