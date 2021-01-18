/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.bigquery.model

import akka.stream.alpakka.googlecloud.bigquery.model.DatasetJsonProtocol.DatasetReference
import akka.stream.alpakka.googlecloud.bigquery.model.ErrorProtoJsonProtocol.ErrorProto
import akka.stream.alpakka.googlecloud.bigquery.model.JobJsonProtocol.JobReference
import akka.stream.alpakka.googlecloud.bigquery.model.TableJsonProtocol.TableSchema
import akka.stream.alpakka.googlecloud.bigquery.scaladsl.Paginated
import akka.stream.alpakka.googlecloud.bigquery.scaladsl.spray.BigQueryRootJsonFormat
import spray.json.{DefaultJsonProtocol, RootJsonFormat}

import scala.collection.immutable.Seq

object QueryJsonProtocol extends DefaultJsonProtocol {

  final case class QueryRequest(query: String,
                                maxResults: Option[Int],
                                defaultDataset: Option[DatasetReference],
                                timeoutMs: Option[Int],
                                dryRun: Option[Boolean],
                                useLegacySql: Option[Boolean],
                                requestId: Option[String])

  implicit val requestFormat: RootJsonFormat[QueryRequest] = jsonFormat7(QueryRequest)

  final case class QueryResponse[+T](schema: Option[TableSchema],
                                     jobReference: JobReference,
                                     totalRows: Option[String],
                                     pageToken: Option[String],
                                     rows: Option[Seq[T]],
                                     totalBytesProcessed: Option[String],
                                     jobComplete: Boolean,
                                     errors: Option[Seq[ErrorProto]],
                                     cacheHit: Option[Boolean],
                                     numDmlAffectedRows: Option[String])

  implicit def responseFormat[T: BigQueryRootJsonFormat]: RootJsonFormat[QueryResponse[T]] =
    jsonFormat10(QueryResponse[T])
  implicit val paginated: Paginated[QueryResponse[Any]] = _.pageToken
}
