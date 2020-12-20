/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.bigquery.model

import akka.stream.alpakka.googlecloud.bigquery.model.DatasetJsonProtocol.DatasetReference
import akka.stream.alpakka.googlecloud.bigquery.model.ErrorProtoJsonProtocol.ErrorProto
import akka.stream.alpakka.googlecloud.bigquery.model.JobJsonProtocol.JobReference
import akka.stream.alpakka.googlecloud.bigquery.model.TableJsonProtocol.TableSchema
import akka.stream.alpakka.googlecloud.bigquery.scaladsl.spray.BigQueryJsonFormat
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
                                     jobReference: Option[JobReference],
                                     totalRows: Option[String],
                                     rows: Option[Seq[T]],
                                     totalBytesProcessed: Option[String],
                                     errors: Option[Seq[ErrorProto]],
                                     cacheHit: Option[Boolean],
                                     numDmlAffectedRows: Option[String])

  implicit def responseFormat[T: BigQueryJsonFormat]: RootJsonFormat[QueryResponse[T]] = jsonFormat8(QueryResponse[T])
}
