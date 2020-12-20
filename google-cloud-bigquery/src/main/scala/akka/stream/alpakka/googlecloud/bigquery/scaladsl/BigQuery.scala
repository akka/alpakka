/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.bigquery.scaladsl

import _root_.spray.json.JsValue
import akka.actor.ClassicActorSystemProvider
import akka.dispatch.ExecutionContexts
import akka.http.scaladsl.marshalling.{Marshal, ToByteStringMarshaller, ToEntityMarshaller}
import akka.http.scaladsl.model.HttpMethods.{DELETE, GET, POST}
import akka.http.scaladsl.model.MediaTypes.`application/json`
import akka.http.scaladsl.model.Uri.Query
import akka.http.scaladsl.model.{HttpEntity, HttpRequest, HttpResponse, RequestEntity}
import akka.http.scaladsl.unmarshalling.{FromEntityUnmarshaller, Unmarshal, Unmarshaller}
import akka.http.scaladsl.util.FastFuture.EnhancedFuture
import akka.stream.OverflowStrategy
import akka.stream.alpakka.googlecloud.bigquery.impl.http.BigQueryHttp
import akka.stream.alpakka.googlecloud.bigquery.impl.paginated.PaginatedRequest
import akka.stream.alpakka.googlecloud.bigquery.impl.{BigQueryExt, LoadJob}
import akka.stream.alpakka.googlecloud.bigquery.model.DatasetJsonProtocol.{
  Dataset,
  DatasetListResponse,
  DatasetReference
}
import akka.stream.alpakka.googlecloud.bigquery.model.JobJsonProtocol.{
  CreateNeverDisposition,
  Job,
  JobConfiguration,
  JobConfigurationLoad,
  JobReference,
  NewlineDelimitedJsonFormat,
  WriteAppendDisposition
}
import akka.stream.alpakka.googlecloud.bigquery.model.QueryJsonProtocol.{QueryRequest, QueryResponse}
import akka.stream.alpakka.googlecloud.bigquery.model.ResponseMetadataJsonProtocol.ResponseMetadata
import akka.stream.alpakka.googlecloud.bigquery.model.TableDataJsonProtocol
import akka.stream.alpakka.googlecloud.bigquery.model.TableDataJsonProtocol.{
  TableDataInsertAllRequest,
  TableDataInsertAllResponse,
  TableDataListResponse
}
import akka.stream.alpakka.googlecloud.bigquery.model.TableJsonProtocol.{Table, TableListResponse, TableReference}
import akka.stream.alpakka.googlecloud.bigquery.scaladsl.schema.TableSchemaWriter
import akka.stream.alpakka.googlecloud.bigquery.scaladsl.spray.SprayJsonSupport
import akka.stream.alpakka.googlecloud.bigquery.{
  BigQueryAttributes,
  BigQueryException,
  BigQuerySettings,
  InsertAllRetryPolicy
}
import akka.stream.scaladsl.{Flow, Keep, Sink, Source}
import akka.util.ByteString
import akka.{Done, NotUsed}
import com.github.ghik.silencer.silent

import java.util.{SplittableRandom, UUID}
import scala.concurrent.Future

/**
 * Scala API to interface with BigQuery.
 */
object BigQuery {

  def apply[T]: BigQuery[T] = new BigQuery[T]

  def settings(implicit system: ClassicActorSystemProvider): BigQuerySettings = BigQueryExt(system).settings

  def settings(prefix: String)(implicit system: ClassicActorSystemProvider): BigQuerySettings =
    BigQueryExt(system).settings(prefix)

  def singleRequest(request: HttpRequest)(implicit system: ClassicActorSystemProvider,
                                          settings: BigQuerySettings): Future[HttpResponse] =
    BigQueryHttp().singleRequestWithOAuth(request)

  def paginatedRequest[Out, Json](
      request: HttpRequest
  )(implicit jsonUnmarshaller: FromEntityUnmarshaller[Json],
    metadataUnmarshaller: Unmarshaller[Json, ResponseMetadata],
    unmarshaller: Unmarshaller[Json, Out]): Source[Out, NotUsed] = PaginatedRequest[Out, Json](request)

  def datasets: Source[Dataset, NotUsed] = datasets()

  def datasets(maxResults: Option[Int] = None,
               all: Option[Boolean] = None,
               filter: Map[String, String] = Map.empty): Source[Dataset, NotUsed] =
    source { settings =>
      import spray.SprayJsonSupport._
      val uri = BigQueryEndpoints.datasets(settings.projectId)
      val query = Query.Empty :+?
        "maxResults" -> maxResults :+?
        "all" -> all :+?
        "filter" -> (if (filter.isEmpty) None else Some(mkFilterParam(filter)))
      paginatedRequest[DatasetListResponse, JsValue](HttpRequest(GET, uri.withQuery(query)))
    }.mapMaterializedValue(_ => NotUsed).mapConcat(_.datasets.fold(List.empty[Dataset])(_.toList))

  def dataset(datasetId: String)(implicit system: ClassicActorSystemProvider,
                                 settings: BigQuerySettings): Future[Dataset] = {
    import BigQueryException._
    import spray.SprayJsonSupport._
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
    import spray.SprayJsonSupport._
    val uri = BigQueryEndpoints.datasets(settings.projectId)
    val entity = HttpEntity(`application/json`, dataset.toJson.compactPrint)
    BigQueryHttp()
      .retryRequestWithOAuth(HttpRequest(POST, uri, entity = entity))
      .flatMap { response =>
        Unmarshal(response.entity).to[Dataset]
      }(system.classicSystem.dispatcher)
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

  def tables(datasetId: String, maxResults: Option[Int] = None): Source[Table, Future[TableListResponse]] =
    source { settings =>
      import spray.SprayJsonSupport._
      val uri = BigQueryEndpoints.tables(settings.projectId, datasetId)
      val query = Query.Empty :+? "maxResults" -> maxResults
      paginatedRequest[TableListResponse, JsValue](HttpRequest(GET, uri.withQuery(query)))
    }.wireTapMat(Sink.head)(Keep.right).mapConcat(_.tables.fold(List.empty[Table])(_.toList))

  def table(datasetId: String, tableId: String)(implicit system: ClassicActorSystemProvider,
                                                settings: BigQuerySettings): Future[Table] = {
    import BigQueryException._
    import spray.SprayJsonSupport._
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
    import spray.SprayJsonSupport._
    val projectId = table.tableReference.projectId.getOrElse(settings.projectId)
    val datasetId = table.tableReference.datasetId
    val uri = BigQueryEndpoints.tables(projectId, datasetId)
    val entity = HttpEntity(`application/json`, table.toJson.compactPrint)
    BigQueryHttp()
      .retryRequestWithOAuth(HttpRequest(POST, uri, entity = entity))
      .flatMap { response =>
        Unmarshal(response.entity).to[Table]
      }(system.classicSystem.dispatcher)
  }

  def deleteTable(datasetId: String, tableId: String)(implicit system: ClassicActorSystemProvider,
                                                      settings: BigQuerySettings): Future[Done] = {
    import BigQueryException._
    val uri = BigQueryEndpoints.table(settings.projectId, datasetId, tableId)
    BigQueryHttp()
      .retryRequestWithOAuth(HttpRequest(DELETE, uri))
      .map(_ => Done)(ExecutionContexts.parasitic)
  }

  def job(jobId: String, location: Option[String] = None)(implicit system: ClassicActorSystemProvider,
                                                          settings: BigQuerySettings): Future[Job] = {
    import BigQueryException._
    import spray.SprayJsonSupport._
    val uri = BigQueryEndpoints.job(settings.projectId, jobId)
    val query = Query.Empty :+? "location" -> location
    BigQueryHttp()
      .retryRequestWithOAuth(HttpRequest(GET, uri.withQuery(query)))
      .flatMap { response =>
        Unmarshal(response.entity).to[Job]
      }(system.classicSystem.dispatcher)
  }

  def cancelJob(jobId: String, location: Option[String] = None)(implicit system: ClassicActorSystemProvider,
                                                                settings: BigQuerySettings): Future[Job] = {
    import BigQueryException._
    import spray.SprayJsonSupport._
    val uri = BigQueryEndpoints.jobCancel(settings.projectId, jobId)
    val query = Query.Empty :+? "location" -> location
    BigQueryHttp()
      .retryRequestWithOAuth(HttpRequest(POST, uri.withQuery(query)))
      .flatMap { response =>
        Unmarshal(response.entity).to[Job]
      }(system.classicSystem.dispatcher)
  }

  def query[Out, Json](
      query: String,
      dryRun: Boolean = false,
      useLegacySql: Boolean = true,
      onCompleteCallback: Option[JobReference] => Future[Done] = BigQueryCallbacks.ignore
  )(implicit jsonUnmarshaller: FromEntityUnmarshaller[Json],
    metadataUnmarshaller: Unmarshaller[Json, ResponseMetadata],
    queryResponseUnmarshaller: Unmarshaller[Json, QueryResponse[Out]]): Source[Out, Future[QueryResponse[Out]]] = {
    val request = QueryRequest(query, None, None, None, Some(dryRun), Some(useLegacySql), None)
    this.query(request, onCompleteCallback)
  }

  def query[Out, Json](query: QueryRequest, onCompleteCallback: Option[JobReference] => Future[Done])(
      implicit jsonUnmarshaller: FromEntityUnmarshaller[Json],
      metadataUnmarshaller: Unmarshaller[Json, ResponseMetadata],
      queryResponseUnmarshaller: Unmarshaller[Json, QueryResponse[Out]]
  ): Source[Out, Future[QueryResponse[Out]]] =
    source { settings =>
      val uri = BigQueryEndpoints.queries(settings.projectId)
      val entity = HttpEntity(`application/json`, query.toJson.compactPrint)
      paginatedRequest[QueryResponse[Out], Json](HttpRequest(POST, uri, entity = entity))
    }.alsoTo(onCompleteCallbackSink(onCompleteCallback))
      .wireTapMat(Sink.head)(Keep.right)
      .buffer(1, OverflowStrategy.backpressure) // Lets the callbacks complete eagerly even if downstream cancels
      .mapConcat(_.rows.fold[List[Out]](Nil)(_.toList))

  private def onCompleteCallbackSink(
      callback: Option[JobReference] => Future[Done]
  ): Sink[QueryResponse[Any], NotUsed] =
    Sink
      .fromMaterializer { (mat, attr) =>
        import mat.executionContext
        Flow[QueryResponse[Any]]
          .map(_.jobReference)
          .wireTapMat(Sink.headOption)(Keep.right)
          .toMat(Sink.ignore) { (jobReference, done) =>
            done.transformWith(_ => jobReference)(ExecutionContexts.parasitic).flatMap { jobReference =>
              callback(jobReference.flatten)
            }
          }
      }
      .mapMaterializedValue(_ => NotUsed)

  def queryResults[Out, Json](
      jobId: String,
      startIndex: Option[Long] = None,
      maxResults: Option[Int] = None,
      timeoutMs: Option[Int] = None
  )(implicit jsonUnmarshaller: FromEntityUnmarshaller[Json],
    metadataUnmarshaller: Unmarshaller[Json, ResponseMetadata],
    queryResponseUnmarshaller: Unmarshaller[Json, QueryResponse[Out]]): Source[Out, Future[QueryResponse[Out]]] =
    source { settings =>
      val uri = BigQueryEndpoints.job(settings.projectId, jobId)
      val query = Query.Empty :+?
        "startIndex" -> startIndex :+?
        "maxResults" -> maxResults :+?
        "timeoutMs" -> timeoutMs
      paginatedRequest[QueryResponse[Out], Json](HttpRequest(GET, uri.withQuery(query)))
    }.wireTapMat(Sink.head)(Keep.right).mapConcat(_.rows.fold[List[Out]](Nil)(_.toList))

  def tableData[Out, Json](datasetId: String,
                           tableId: String,
                           startIndex: Option[Long] = None,
                           maxResults: Option[Int] = None,
                           selectedFields: Seq[String] = Seq.empty)(
      implicit jsonUnmarshaller: FromEntityUnmarshaller[Json],
      metadataUnmarshaller: Unmarshaller[Json, ResponseMetadata],
      tableDataListUnmarshaller: Unmarshaller[Json, TableDataListResponse[Out]]
  ): Source[Out, Future[TableDataListResponse[Out]]] =
    source { settings =>
      val uri = BigQueryEndpoints.tableData(settings.projectId, datasetId, tableId)
      val query = Query.Empty :+?
        "startIndex" -> startIndex :+?
        "maxResults" -> maxResults :+?
        "selectedFields" -> (if (selectedFields.isEmpty) None else Some(selectedFields.mkString(",")))
      paginatedRequest[TableDataListResponse[Out], Json](HttpRequest(GET, uri.withQuery(query)))
    }.wireTapMat(Sink.head)(Keep.right).mapConcat(_.rows.fold[List[Out]](Nil)(_.toList))

  def insertAll[In](
      datasetId: String,
      tableId: String,
      retryPolicy: InsertAllRetryPolicy,
      templateSuffix: Option[String] = None
  )(implicit marshaller: ToEntityMarshaller[TableDataInsertAllRequest[In]]): Sink[Seq[In], NotUsed] = {
    val requests = Flow[Seq[In]].statefulMapConcat { () =>
      val randomGen = new SplittableRandom

      xs => {
        val rows = xs.map { x =>
          val insertId =
            if (retryPolicy.deduplicate)
              Some(randomUUID(randomGen).toString)
            else
              None
          TableDataJsonProtocol.Row(insertId, x)
        }

        TableDataInsertAllRequest(None, None, templateSuffix, rows) :: Nil
      }
    }

    val errorSink = Sink.foreach[TableDataInsertAllResponse] { response =>
      response.insertErrors
        .flatMap(_.headOption)
        .flatMap(_.errors)
        .flatMap(_.headOption)
        .foreach(error => throw BigQueryException(error))
    }

    requests.via(insertAll(tableId, datasetId, retryPolicy.retry)).to(errorSink)
  }

  def insertAll[In](datasetId: String, tableId: String, retryFailedRequests: Boolean)(
      implicit marshaller: ToEntityMarshaller[TableDataInsertAllRequest[In]]
  ): Flow[TableDataInsertAllRequest[In], TableDataInsertAllResponse, NotUsed] =
    Flow
      .fromMaterializer { (mat, attr) =>
        import BigQueryException._
        import SprayJsonSupport._
        import mat.executionContext
        implicit val system = mat.system
        implicit val settings = BigQueryAttributes.resolveSettings(attr, mat)

        val uri = BigQueryEndpoints.tableDataInsertAll(settings.projectId, datasetId, tableId)
        val request = HttpRequest(POST, uri)

        val http = BigQueryHttp()
        val requestWithOAuth =
          if (retryFailedRequests)
            http.retryRequestWithOAuth(_)
          else
            http.singleRequestWithOAuthOrFail(_)

        Flow[TableDataInsertAllRequest[In]]
          .mapAsync(1)(Marshal(_).to[RequestEntity])
          .map(request.withEntity)
          .mapAsync(1)(requestWithOAuth)
          .mapAsync(1)(Unmarshal(_).to[TableDataInsertAllResponse])
      }
      .mapMaterializedValue(_ => NotUsed)

  def insertAllAsync[In](datasetId: String,
                         tableId: String)(implicit marshaller: ToByteStringMarshaller[In]): Flow[In, Job, NotUsed] =
    Flow
      .fromMaterializer { (mat, attr) =>
        import SprayJsonSupport._
        import mat.executionContext
        val settings = BigQueryAttributes.resolveSettings(attr, mat)
        import settings.loadJobSettings.perTableQuota

        val job = Job(
          Some(
            JobConfiguration(
              Some(
                JobConfigurationLoad(
                  None,
                  Some(TableReference(Some(settings.projectId), datasetId, tableId)),
                  Some(CreateNeverDisposition),
                  Some(WriteAppendDisposition),
                  Some(NewlineDelimitedJsonFormat)
                )
              )
            )
          ),
          None,
          None
        )

        val newline = ByteString("\n")
        val jobFlow = Flow[In]
          .takeWithin(perTableQuota)
          .mapAsync(1)(Marshal(_).to[ByteString])
          .map(_ ++ newline)
          .via(createLoadJob(job))
          .reduce(Keep.right)

        Flow[In]
          .groupBy(1, _ => (), allowClosedSubstreamRecreation = true)
          .via(jobFlow)
          .concatSubstreams
      }
      .mapMaterializedValue(_ => NotUsed)

  @silent("shadow")
  def createLoadJob[Job](job: Job)(implicit marshaller: ToEntityMarshaller[Job],
                                   unmarshaller: FromEntityUnmarshaller[Job]): Flow[ByteString, Job, NotUsed] =
    Flow
      .fromMaterializer { (mat, attr) =>
        import mat.executionContext
        implicit val settings = BigQueryAttributes.resolveSettings(attr, mat)
        val uri = BigQueryMediaEndpoints.jobs(settings.projectId)
        Flow.futureFlow {
          Marshal(job)
            .to[RequestEntity]
            .fast
            .map { entity =>
              LoadJob(HttpRequest(POST, uri, entity = entity))
            }(ExecutionContexts.parasitic)
        }
      }
      .mapMaterializedValue(_ => NotUsed)

  private def source[Out, Mat](f: BigQuerySettings => Source[Out, Mat]): Source[Out, Future[Mat]] =
    Source.fromMaterializer { (mat, attr) =>
      f(BigQueryAttributes.resolveSettings(attr, mat))
    }

  private implicit final class QueryAddOption(val query: Query) extends AnyVal {
    def :+?(kv: (String, Option[Any])): Query = kv._2.fold(query)(v => Query.Cons(kv._1, v.toString, query))
  }

  private def mkFilterParam(filter: Map[String, String]): String =
    filter.view
      .map {
        case (key, value) =>
          val colonValue = if (value.isEmpty) "" else s":$value"
          s"label.$key$colonValue"
      }
      .mkString(" ")

  private def randomUUID(randomGen: SplittableRandom): UUID = {
    var msb = randomGen.nextLong()
    var lsb = randomGen.nextLong()
    msb &= 0xFFFFFFFFFFFF0FFFL // clear version
    msb |= 0x0000000000004000L // set to version 4
    lsb &= 0x3FFFFFFFFFFFFFFFL // clear variant
    lsb |= 0x8000000000000000L // set to IETF variant
    new UUID(msb, lsb)
  }
}

final class BigQuery[T] private () {

  def createTable(datasetId: String, tableId: String)(
      implicit system: ClassicActorSystemProvider,
      settings: BigQuerySettings,
      schemaWriter: TableSchemaWriter[T]
  ): Future[Table] = BigQuery.createTable(datasetId, tableId)

  def paginatedRequest[Json](request: HttpRequest)(implicit jsonUnmarshaller: FromEntityUnmarshaller[Json],
                                                   metadataUnmarshaller: Unmarshaller[Json, ResponseMetadata],
                                                   unmarshaller: Unmarshaller[Json, T]): Source[T, NotUsed] =
    BigQuery.paginatedRequest[T, Json](request)

  def query[Json](
      query: String,
      dryRun: Boolean = false,
      useLegacySql: Boolean = true,
      onCompleteCallback: Option[JobReference] => Future[Done] = BigQueryCallbacks.ignore
  )(implicit jsonUnmarshaller: FromEntityUnmarshaller[Json],
    metadataUnmarshaller: Unmarshaller[Json, ResponseMetadata],
    queryResponseUnmarshaller: Unmarshaller[Json, QueryResponse[T]]): Source[T, Future[QueryResponse[T]]] =
    BigQuery.query(query, dryRun, useLegacySql, onCompleteCallback)

  def query[Json](query: QueryRequest, onCompleteCallback: Option[JobReference] => Future[Done])(
      implicit jsonUnmarshaller: FromEntityUnmarshaller[Json],
      metadataUnmarshaller: Unmarshaller[Json, ResponseMetadata],
      queryResponseUnmarshaller: Unmarshaller[Json, QueryResponse[T]]
  ): Source[T, Future[QueryResponse[T]]] = BigQuery.query(query, onCompleteCallback)

  def queryResults[Json](jobId: String,
                         startIndex: Option[Long] = None,
                         maxResults: Option[Int] = None,
                         timeoutMs: Option[Int] = None)(
      implicit jsonUnmarshaller: FromEntityUnmarshaller[Json],
      metadataUnmarshaller: Unmarshaller[Json, ResponseMetadata],
      queryResponseUnmarshaller: Unmarshaller[Json, QueryResponse[T]]
  ): Source[T, Future[QueryResponse[T]]] = BigQuery.queryResults(jobId, startIndex, maxResults, timeoutMs)

  def tableData[Json](datasetId: String,
                      tableId: String,
                      startIndex: Option[Long] = None,
                      maxResults: Option[Int] = None,
                      selectedFields: Seq[String] = Seq.empty)(
      implicit jsonUnmarshaller: FromEntityUnmarshaller[Json],
      metadataUnmarshaller: Unmarshaller[Json, ResponseMetadata],
      tableDataListUnmarshaller: Unmarshaller[Json, TableDataListResponse[T]]
  ): Source[T, Future[TableDataListResponse[T]]] =
    BigQuery.tableData(datasetId, tableId, startIndex, maxResults, selectedFields)

  def insertAll(
      datasetId: String,
      tableId: String,
      retryPolicy: InsertAllRetryPolicy,
      templateSuffix: Option[String] = None
  )(implicit marshaller: ToEntityMarshaller[TableDataInsertAllRequest[T]]): Sink[Seq[T], NotUsed] =
    BigQuery.insertAll(datasetId, tableId, retryPolicy, templateSuffix)

  def insertAll(datasetId: String, tableId: String, retryFailedRequests: Boolean)(
      implicit marshaller: ToEntityMarshaller[TableDataInsertAllRequest[T]]
  ): Flow[TableDataInsertAllRequest[T], TableDataInsertAllResponse, NotUsed] =
    BigQuery.insertAll(datasetId, tableId, retryFailedRequests)

  def insertAllAsync(datasetId: String,
                     tableId: String)(implicit marshaller: ToByteStringMarshaller[T]): Flow[T, Job, NotUsed] =
    BigQuery.insertAllAsync(datasetId, tableId)
}
