/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.bigquery.scaladsl

import akka.{Done, NotUsed}
import akka.actor.ClassicActorSystemProvider
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import akka.http.scaladsl.marshalling.Marshal
import akka.http.scaladsl.model.HttpMethods.{DELETE, GET, POST}
import akka.http.scaladsl.model.{HttpRequest, RequestEntity}
import akka.http.scaladsl.model.Uri.Query
import akka.http.scaladsl.unmarshalling.Unmarshal
import akka.stream.alpakka.googlecloud.bigquery.{BigQueryEndpoints, BigQueryException, BigQuerySettings}
import akka.stream.alpakka.googlecloud.bigquery.impl.http.BigQueryHttp
import akka.stream.alpakka.googlecloud.bigquery.model.DatasetJsonProtocol.{
  Dataset,
  DatasetListResponse,
  DatasetReference
}
import akka.stream.scaladsl.Source

import scala.concurrent.Future

private[scaladsl] trait BigQueryDatasets { this: BigQueryRest =>

  def datasets: Source[Dataset, NotUsed] = datasets()

  def datasets(maxResults: Option[Int] = None,
               all: Option[Boolean] = None,
               filter: Map[String, String] = Map.empty): Source[Dataset, NotUsed] =
    source { settings =>
      import SprayJsonSupport._
      val uri = BigQueryEndpoints.datasets(settings.projectId)
      val query = Query.Empty :+?
        "maxResults" -> maxResults :+?
        "all" -> all :+?
        "filter" -> (if (filter.isEmpty) None else Some(mkFilterParam(filter)))
      paginatedRequest[DatasetListResponse](HttpRequest(GET, uri.withQuery(query)))
    }.mapMaterializedValue(_ => NotUsed).mapConcat(_.datasets.fold(List.empty[Dataset])(_.toList))

  def dataset(datasetId: String)(implicit system: ClassicActorSystemProvider,
                                 settings: BigQuerySettings): Future[Dataset] = {
    import BigQueryException._
    import SprayJsonSupport._
    val uri = BigQueryEndpoints.dataset(settings.projectId, datasetId)
    BigQueryHttp()
      .retryRequestWithOAuth(HttpRequest(GET, uri))
      .flatMap { response =>
        Unmarshal(response.entity).to[Dataset]
      }(system.classicSystem.dispatcher)
  }

  def createDataset(datasetId: String)(implicit system: ClassicActorSystemProvider,
                                       settings: BigQuerySettings): Future[Dataset] = {
    val dataset = Dataset(DatasetReference(datasetId, None), None, None, None)
    createDataset(dataset)
  }

  def createDataset(dataset: Dataset)(implicit system: ClassicActorSystemProvider,
                                      settings: BigQuerySettings): Future[Dataset] = {
    import BigQueryException._
    import SprayJsonSupport._
    implicit val ec = system.classicSystem.dispatcher
    val uri = BigQueryEndpoints.datasets(settings.projectId)
    for {
      entity <- Marshal(dataset).to[RequestEntity]
      request = HttpRequest(POST, uri, entity = entity)
      response <- BigQueryHttp().retryRequestWithOAuth(request)
      dataset <- Unmarshal(response.entity).to[Dataset]
    } yield dataset
  }

  def deleteDataset(datasetId: String, deleteContents: Boolean = false)(implicit system: ClassicActorSystemProvider,
                                                                        settings: BigQuerySettings): Future[Done] = {
    import BigQueryException._
    val uri = BigQueryEndpoints.dataset(settings.projectId, datasetId)
    val query = Query("deleteContents" -> deleteContents.toString)
    BigQueryHttp()
      .retryRequestWithOAuth(HttpRequest(DELETE, uri.withQuery(query)))
      .map(_ => Done)(system.classicSystem.dispatcher)
  }

}
