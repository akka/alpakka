/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.bigquery.model

import akka.stream.alpakka.googlecloud.bigquery.model.ErrorProtoJsonProtocol.ErrorProto
import akka.stream.alpakka.googlecloud.bigquery.scaladsl.Paginated
import akka.stream.alpakka.googlecloud.bigquery.scaladsl.spray.BigQueryJsonFormat
import spray.json.{DefaultJsonProtocol, JsonFormat, RootJsonFormat}

object TableDataJsonProtocol extends DefaultJsonProtocol {

  final case class TableDataListResponse[+T](totalRows: String, pageToken: Option[String], rows: Option[Seq[T]])

  implicit def listResponseFormat[T: BigQueryJsonFormat]: RootJsonFormat[TableDataListResponse[T]] =
    jsonFormat3(TableDataListResponse[T])
  implicit val paginated: Paginated[TableDataListResponse[Any]] = _.pageToken

  final case class TableDataInsertAllRequest[+T](skipInvalidRows: Option[Boolean],
                                                 ignoreUnknownValues: Option[Boolean],
                                                 templateSuffix: Option[String],
                                                 rows: Seq[Row[T]])
  final case class Row[+T](insertId: Option[String], json: T)

  implicit def rowFormat[T: BigQueryJsonFormat]: JsonFormat[Row[T]] = jsonFormat2(Row[T])
  implicit def insertAllRequestFormat[T: BigQueryJsonFormat]: RootJsonFormat[TableDataInsertAllRequest[T]] =
    jsonFormat4(TableDataInsertAllRequest[T])

  final case class TableDataInsertAllResponse(insertErrors: Option[Seq[InsertError]])
  final case class InsertError(index: Long, errors: Option[Seq[ErrorProto]])

  implicit val insertErrorFormat: JsonFormat[InsertError] = jsonFormat2(InsertError)
  implicit val insertAllResponseFormat: RootJsonFormat[TableDataInsertAllResponse] =
    jsonFormat1(TableDataInsertAllResponse)
}
