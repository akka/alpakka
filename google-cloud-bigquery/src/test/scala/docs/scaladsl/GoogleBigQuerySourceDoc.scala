/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.scaladsl

//#imports
import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.alpakka.googlecloud.bigquery.BigQueryConfig
import akka.stream.alpakka.googlecloud.bigquery.client.BigQueryCommunicationHelper
import akka.stream.alpakka.googlecloud.bigquery.client.TableDataQueryJsonProtocol.Field
import akka.stream.alpakka.googlecloud.bigquery.client.TableListQueryJsonProtocol.QueryTableModel
import akka.stream.alpakka.googlecloud.bigquery.scaladsl.BigQueryJsonProtocol._
import akka.stream.alpakka.googlecloud.bigquery.scaladsl.SprayJsonSupport._
import akka.stream.alpakka.googlecloud.bigquery.scaladsl.{BigQueryCallbacks, GoogleBigQuerySource}
import akka.stream.scaladsl.{Sink, Source}
import spray.json.RootJsonFormat

import scala.concurrent.Future
//#imports

class GoogleBigQuerySourceDoc {

  implicit val system = ActorSystem()
  implicit val executionContext = system.dispatcher

  //#init-config
  val config = BigQueryConfig("project@test.test", "privateKeyFromGoogle", "projectID", "bigQueryDatasetName")
  //#init-config

  //#list-tables-and-fields
  val tables: Future[Seq[QueryTableModel]] = GoogleBigQuerySource.listTables(config).runWith(Sink.seq).map(_.flatten)
  val fields: Future[Seq[Field]] = GoogleBigQuerySource.listFields("myTable", config).runWith(Sink.seq).map(_.flatten)
  //#list-tables-and-fields

  //#csv-style
  val userCsvLikeStream: Source[Seq[String], NotUsed] =
    GoogleBigQuerySource.runQueryCsvStyle("SELECT uid, name FROM bigQueryDatasetName.myTable",
                                          BigQueryCallbacks.tryToStopJob(config),
                                          config)
  //#csv-style

  //#run-query
  case class User(uid: String, name: String)
  implicit val userFormatter = bigQueryJsonFormat2(User)

  val userStream: Source[User, NotUsed] =
    GoogleBigQuerySource[User]
      .runQuery("SELECT uid, name FROM bigQueryDatasetName.myTable", BigQueryCallbacks.ignore, config)
  //#run-query

  //#dry-run
  case class DryRunResponse(totalBytesProcessed: String, jobComplete: Boolean, cacheHit: Boolean)
  implicit val dryRunFormat: RootJsonFormat[DryRunResponse] = jsonFormat3(DryRunResponse)

  val request = BigQueryCommunicationHelper.createQueryRequest("SELECT uid, name FROM bigQueryDatasetName.myTable",
                                                               config.projectId,
                                                               dryRun = true)

  val dryRunStream: Source[DryRunResponse, NotUsed] =
    GoogleBigQuerySource[DryRunResponse].raw(request, BigQueryCallbacks.ignore, config)
  //#dry-run
}
