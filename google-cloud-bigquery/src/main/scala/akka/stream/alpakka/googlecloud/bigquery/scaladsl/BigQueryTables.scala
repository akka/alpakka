/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.bigquery.scaladsl

import akka.Done
import akka.actor.ClassicActorSystemProvider
import akka.dispatch.ExecutionContexts
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import akka.http.scaladsl.marshalling.Marshal
import akka.http.scaladsl.model.HttpMethods.{DELETE, GET, POST}
import akka.http.scaladsl.model.{HttpRequest, RequestEntity}
import akka.http.scaladsl.model.Uri.Query
import akka.http.scaladsl.unmarshalling.Unmarshal
import akka.stream.alpakka.googlecloud.bigquery.{BigQueryEndpoints, BigQueryException, BigQuerySettings}
import akka.stream.alpakka.googlecloud.bigquery.impl.http.BigQueryHttp
import akka.stream.alpakka.googlecloud.bigquery.model.TableJsonProtocol.{Table, TableListResponse, TableReference}
import akka.stream.alpakka.googlecloud.bigquery.scaladsl.schema.TableSchemaWriter
import akka.stream.scaladsl.{Keep, Sink, Source}

import scala.concurrent.Future

private[scaladsl] trait BigQueryTables { this: BigQueryRest =>

  /**
   * Lists all tables in the specified dataset.
   * @see [[https://cloud.google.com/bigquery/docs/reference/rest/v2/tables/list BigQuery reference]]
   *
   * @param datasetId dataset ID of the tables to list
   * @param maxResults the maximum number of results to return in a single response page
   * @return a [[akka.stream.scaladsl.Source]] that emits each [[akka.stream.alpakka.googlecloud.bigquery.model.TableJsonProtocol.Table]] in the dataset and materializes a [[scala.concurrent.Future]] containing the [[akka.stream.alpakka.googlecloud.bigquery.model.TableJsonProtocol.TableListResponse]]
   */
  def tables(datasetId: String, maxResults: Option[Int] = None): Source[Table, Future[TableListResponse]] =
    source { settings =>
      import SprayJsonSupport._
      val uri = BigQueryEndpoints.tables(settings.projectId, datasetId)
      val query = Query.Empty :+? "maxResults" -> maxResults
      paginatedRequest[TableListResponse](HttpRequest(GET, uri.withQuery(query)))
    }.wireTapMat(Sink.head)(Keep.right).mapConcat(_.tables.fold(List.empty[Table])(_.toList))

  /**
   * Gets the specified table resource. This method does not return the data in the table, it only returns the table resource, which describes the structure of this table.
   * @see [[https://cloud.google.com/bigquery/docs/reference/rest/v2/tables/get BigQuery reference]]
   *
   * @param datasetId dataset ID of the requested table
   * @param tableId table ID of the requested table
   * @return a [[scala.concurrent.Future]] containing the [[akka.stream.alpakka.googlecloud.bigquery.model.TableJsonProtocol.Table]]
   */
  def table(datasetId: String, tableId: String)(implicit system: ClassicActorSystemProvider,
                                                settings: BigQuerySettings): Future[Table] = {
    import BigQueryException._
    import SprayJsonSupport._
    val uri = BigQueryEndpoints.table(settings.projectId, datasetId, tableId)
    BigQueryHttp()
      .retryRequestWithOAuth(HttpRequest(GET, uri))
      .flatMap { response =>
        Unmarshal(response.entity).to[Table]
      }(system.classicSystem.dispatcher)
  }

  /**
   * Creates a new, empty table in the dataset.
   * @see [[https://cloud.google.com/bigquery/docs/reference/rest/v2/tables/insert BigQuery reference]]
   *
   * @param datasetId dataset ID of the new table
   * @param tableId table ID of the new table
   * @tparam T the data model for the records of this table
   * @return a [[scala.concurrent.Future]] containing the [[akka.stream.alpakka.googlecloud.bigquery.model.TableJsonProtocol.Table]]
   */
  def createTable[T](datasetId: String, tableId: String)(
      implicit system: ClassicActorSystemProvider,
      settings: BigQuerySettings,
      schemaWriter: TableSchemaWriter[T]
  ): Future[Table] = {
    val table = Table(TableReference(None, datasetId, tableId), None, Some(schemaWriter.write), None, None)
    createTable(table)
  }

  /**
   * Creates a new, empty table in the dataset.
   * @see [[https://cloud.google.com/bigquery/docs/reference/rest/v2/tables/insert BigQuery reference]]
   *
   * @param table the [[akka.stream.alpakka.googlecloud.bigquery.model.TableJsonProtocol.Table]] to create
   * @return a [[scala.concurrent.Future]] containing the [[akka.stream.alpakka.googlecloud.bigquery.model.TableJsonProtocol.Table]]
   */
  def createTable(table: Table)(implicit system: ClassicActorSystemProvider,
                                settings: BigQuerySettings): Future[Table] = {
    import BigQueryException._
    import SprayJsonSupport._
    implicit val ec = system.classicSystem.dispatcher
    val projectId = table.tableReference.projectId.getOrElse(settings.projectId)
    val datasetId = table.tableReference.datasetId
    val uri = BigQueryEndpoints.tables(projectId, datasetId)
    for {
      entity <- Marshal(table).to[RequestEntity]
      request = HttpRequest(POST, uri, entity = entity)
      response <- BigQueryHttp().retryRequestWithOAuth(request)
      table <- Unmarshal(response.entity).to[Table]
    } yield table
  }

  /**
   * Deletes the specified table from the dataset. If the table contains data, all the data will be deleted.
   * @see [[https://cloud.google.com/bigquery/docs/reference/rest/v2/tables/delete BigQuery reference]]
   *
   * @param datasetId dataset ID of the table to delete
   * @param tableId table ID of the table to delete
   * @return a [[scala.concurrent.Future]] containing [[akka.Done]]
   */
  def deleteTable(datasetId: String, tableId: String)(implicit system: ClassicActorSystemProvider,
                                                      settings: BigQuerySettings): Future[Done] = {
    import BigQueryException._
    val uri = BigQueryEndpoints.table(settings.projectId, datasetId, tableId)
    BigQueryHttp()
      .retryRequestWithOAuth(HttpRequest(DELETE, uri))
      .map(_ => Done)(ExecutionContexts.parasitic)
  }

}
