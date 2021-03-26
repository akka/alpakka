/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.bigquery.scaladsl

import akka.Done
import akka.http.scaladsl.unmarshalling.{FromEntityUnmarshaller, Unmarshaller}
import akka.stream.alpakka.google.scaladsl.Google
import akka.stream.alpakka.google.{GoogleAttributes, GoogleSettings}
import akka.stream.scaladsl.Source

import scala.concurrent.Future

private[scaladsl] trait BigQueryRest extends Google {

  // Helper methods

  protected[this] def source[Out, Mat](f: GoogleSettings => Source[Out, Mat]): Source[Out, Future[Mat]] =
    Source.fromMaterializer { (mat, attr) =>
      f(GoogleAttributes.resolveSettings(mat, attr))
    }

  protected[this] def mkFilterParam(filter: Map[String, String]): String =
    filter.view
      .map {
        case (key, value) =>
          val colonValue = if (value.isEmpty) "" else s":$value"
          s"label.$key$colonValue"
      }
      .mkString(" ")

  protected[this] implicit val doneUnmarshaller: FromEntityUnmarshaller[Done] =
    Unmarshaller.withMaterializer(_ => implicit mat => _.discardBytes().future)
}
