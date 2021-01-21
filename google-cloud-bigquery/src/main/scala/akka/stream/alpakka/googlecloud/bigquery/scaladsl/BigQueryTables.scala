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

  def tables(datasetId: String, maxResults: Option[Int] = None): Source[Table, Future[TableListResponse]] =
    source { settings =>
      import SprayJsonSupport._
      val uri = BigQueryEndpoints.tables(settings.projectId, datasetId)
      val query = Query.Empty :+? "maxResults" -> maxResults
      paginatedRequest[TableListResponse](HttpRequest(GET, uri.withQuery(query)))
    }.wireTapMat(Sink.head)(Keep.right).mapConcat(_.tables.fold(List.empty[Table])(_.toList))

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

  def createTable[T](datasetId: String, tableId: String)(
      implicit system: ClassicActorSystemProvider,
      settings: BigQuerySettings,
      schemaWriter: TableSchemaWriter[T]
  ): Future[Table] = {
    val table = Table(TableReference(None, datasetId, tableId), None, Some(schemaWriter.write), None, None)
    createTable(table)
  }

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

  def deleteTable(datasetId: String, tableId: String)(implicit system: ClassicActorSystemProvider,
                                                      settings: BigQuerySettings): Future[Done] = {
    import BigQueryException._
    val uri = BigQueryEndpoints.table(settings.projectId, datasetId, tableId)
    BigQueryHttp()
      .retryRequestWithOAuth(HttpRequest(DELETE, uri))
      .map(_ => Done)(ExecutionContexts.parasitic)
  }

}
