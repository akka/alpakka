/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package docs.scaladsl

/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */
//#imports
import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Source
import spray.json.JsonFormat

import scala.concurrent.Future
import akka.stream.alpakka.google.cloud.bigquery.impl.client.TableDataQueryJsonProtocol.Field
import akka.stream.alpakka.google.cloud.bigquery.impl.client.TableListQueryJsonProtocol.QueryTableModel
import spray.json.DefaultJsonProtocol._
import spray.json.JsObject
import scala.util.Try
import akka.stream.alpakka.google.cloud.bigquery.BigQueryCommunicationHelper
import akka.stream.alpakka.google.cloud.bigquery.scaladsl.GoogleBigQuerySource
//#imports

class GoogleBigQuerySourceDoc {

  //#init-mat
  implicit val system = ActorSystem()
  implicit val mat = ActorMaterializer()
  implicit val executionContext = system.dispatcher
  //#init-mat

  //#init-config
  val config = GoogleBigQuerySource.createProjectConfig("project@test.test",
                                                        "privateKeyFromGoogle",
                                                        "projectID",
                                                        "bigQueryDatasetName")
  //#init-config

  //#list-tables-and-fields
  val tables: Future[Seq[QueryTableModel]] = GoogleBigQuerySource.listTables(config)
  val fields: Future[Seq[Field]] = GoogleBigQuerySource.listFields("myTable", config)
  //#list-tables-and-fields

  //#csv-style
  val userCsvLikeStream: Source[Seq[String], NotUsed] =
    GoogleBigQuerySource.runQueryCsvStyle("SELECT uid, name FROM bigQueryDatasetName.myTable", config)
  //#csv-style

  //#run-query
  case class User(uid: String, name: String)
  implicit val userFormatter = jsonFormat2(User)

  def parserFn(result: JsObject): Option[User] = Try(result.convertTo[User]).toOption
  val userStream: Source[User, NotUsed] =
    GoogleBigQuerySource.runQuery("SELECT uid, name FROM bigQueryDatasetName.myTable", parserFn, config)
  //#run-query

  //#dry-run
  case class DryRunResponse(totalBytesProcessed: String, jobComplete: Boolean, cacheHit: Boolean)
  implicit val dryRunFormat: JsonFormat[DryRunResponse] = jsonFormat3(DryRunResponse)

  def dryRunParser(result: JsObject): Option[DryRunResponse] = Try(result.convertTo[DryRunResponse]).toOption

  val request = BigQueryCommunicationHelper.createQueryRequest("SELECT uid, name FROM bigQueryDatasetName.myTable",
                                                               config.projectId,
                                                               dryRun = true)

  val dryRunStream = GoogleBigQuerySource.raw(request, dryRunParser, config.session)
  //#dry-run
}
