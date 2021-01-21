/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.bigquery.scaladsl

import akka.{Done, NotUsed}
import akka.actor.ClassicActorSystemProvider
import akka.dispatch.ExecutionContexts
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import akka.http.scaladsl.marshalling.{Marshal, ToEntityMarshaller}
import akka.http.scaladsl.model.HttpMethods.{GET, POST}
import akka.http.scaladsl.model.{HttpEntity, HttpRequest, RequestEntity}
import akka.http.scaladsl.model.Uri.Query
import akka.http.scaladsl.unmarshalling.{FromEntityUnmarshaller, Unmarshal}
import akka.http.scaladsl.util.FastFuture.EnhancedFuture
import akka.stream.alpakka.googlecloud.bigquery.impl.LoadJob
import akka.stream.{OverflowStrategy, RestartSettings}
import akka.stream.alpakka.googlecloud.bigquery.{
  BigQueryAttributes,
  BigQueryEndpoints,
  BigQueryException,
  BigQueryMediaEndpoints,
  BigQuerySettings
}
import akka.stream.alpakka.googlecloud.bigquery.impl.http.BigQueryHttp
import akka.stream.alpakka.googlecloud.bigquery.model.JobJsonProtocol.{
  CreateNeverDisposition,
  Job,
  JobCancelResponse,
  JobConfiguration,
  JobConfigurationLoad,
  JobReference,
  NewlineDelimitedJsonFormat,
  WriteAppendDisposition
}
import akka.stream.alpakka.googlecloud.bigquery.model.QueryJsonProtocol.{QueryRequest, QueryResponse}
import akka.stream.alpakka.googlecloud.bigquery.model.TableJsonProtocol.TableReference
import akka.stream.scaladsl.{Flow, Keep, RestartSource, Sink, Source}
import akka.util.ByteString

import scala.concurrent.{Future, Promise}
import scala.concurrent.duration.FiniteDuration
import scala.util.{Failure, Success}

private[scaladsl] trait BigQueryJobs { this: BigQueryRest =>

  def job(jobId: String, location: Option[String] = None)(implicit system: ClassicActorSystemProvider,
                                                          settings: BigQuerySettings): Future[Job] = {
    import BigQueryException._
    import SprayJsonSupport._
    val uri = BigQueryEndpoints.job(settings.projectId, jobId)
    val query = Query.Empty :+? "location" -> location
    BigQueryHttp()
      .retryRequestWithOAuth(HttpRequest(GET, uri.withQuery(query)))
      .flatMap { response =>
        Unmarshal(response.entity).to[Job]
      }(system.classicSystem.dispatcher)
  }

  def cancelJob(
      jobId: String,
      location: Option[String] = None
  )(implicit system: ClassicActorSystemProvider, settings: BigQuerySettings): Future[JobCancelResponse] = {
    import BigQueryException._
    import SprayJsonSupport._
    implicit val ec = system.classicSystem.dispatcher
    val uri = BigQueryEndpoints.jobCancel(settings.projectId, jobId)
    val query = Query.Empty :+? "location" -> location
    BigQueryHttp()
      .retryRequestWithOAuth(HttpRequest(POST, uri.withQuery(query)))
      .flatMap { response =>
        Unmarshal(response.entity).to[JobCancelResponse]
      }(system.classicSystem.dispatcher)
  }

  def query[Out](
      query: String,
      dryRun: Boolean = false,
      useLegacySql: Boolean = true,
      onCompleteCallback: Option[JobReference] => Future[Done] = BigQueryCallbacks.ignore
  )(implicit um: FromEntityUnmarshaller[QueryResponse[Out]]): Source[Out, Future[QueryResponse[Out]]] = {
    val request = QueryRequest(query, None, None, None, Some(dryRun), Some(useLegacySql), None)
    this.query(request, onCompleteCallback)
  }

  def query[Out](query: QueryRequest, onCompleteCallback: Option[JobReference] => Future[Done])(
      implicit um: FromEntityUnmarshaller[QueryResponse[Out]]
  ): Source[Out, Future[QueryResponse[Out]]] =
    Source
      .fromMaterializer { (mat, attr) =>
        import BigQueryException._
        import SprayJsonSupport._
        import mat.executionContext
        implicit val system = mat.system
        implicit val settings = BigQueryAttributes.resolveSettings(attr, mat)

        Source.lazyFutureSource { () =>
          for {
            entity <- Marshal(query).to[RequestEntity]
            initialRequest = HttpRequest(POST, BigQueryEndpoints.queries(settings.projectId), entity = entity)
            response <- BigQueryHttp().retryRequestWithOAuth(initialRequest)
            initialQueryResponse <- Unmarshal(response.entity).to[QueryResponse[Out]]
          } yield {

            val jobId = initialQueryResponse.jobReference.jobId.getOrElse {
              throw BigQueryException("Query response did not contain job id.")
            }

            val head =
              if (initialQueryResponse.jobComplete)
                Source.single(initialQueryResponse)
              else
                Source.empty

            val tail =
              if (initialQueryResponse.jobComplete & initialQueryResponse.pageToken.isEmpty)
                Source.empty
              else {
                import settings.retrySettings._
                val pages = queryResultsPages[Out](jobId,
                                                   None,
                                                   query.maxResults,
                                                   query.timeoutMs,
                                                   initialQueryResponse.jobReference.location,
                                                   initialQueryResponse.pageToken)
                  .map(Success(_))
                  .recover { case ex => Failure(ex) } // Allows upstream failures to escape the RestartSource
                  .map { queryResponse =>
                    if (queryResponse.toOption.forall(_.jobComplete))
                      queryResponse
                    else
                      throw BigQueryException("Query job not complete.")
                  }
                val restartSettings = RestartSettings(minBackoff, maxBackoff, randomFactor)
                RestartSource.onFailuresWithBackoff(restartSettings)(() => pages).map(_.get)
              }

            head.concat(tail)
          }

        }
      }
      .alsoTo(onCompleteCallbackSink(onCompleteCallback))
      .wireTapMat(Sink.head)(Keep.right)
      .buffer(1, OverflowStrategy.backpressure) // Lets the callbacks complete eagerly even if downstream cancels
      .mapConcat(_.rows.fold[List[Out]](Nil)(_.toList))

  private def onCompleteCallbackSink[T](
      callback: Option[JobReference] => Future[Done]
  ): Sink[QueryResponse[T], NotUsed] =
    Sink
      .fromMaterializer { (mat, attr) =>
        import mat.executionContext
        Flow[QueryResponse[T]]
          .map(_.jobReference)
          .wireTapMat(Sink.headOption)(Keep.right)
          .toMat(Sink.ignore) { (jobReference, done) =>
            done.transformWith(_ => jobReference)(ExecutionContexts.parasitic).flatMap(callback)
          }
      }
      .mapMaterializedValue(_ => NotUsed)

  def queryResults[Out](
      jobId: String,
      startIndex: Option[Long] = None,
      maxResults: Option[Int] = None,
      timeout: Option[FiniteDuration] = None,
      location: Option[String] = None
  )(
      implicit um: FromEntityUnmarshaller[QueryResponse[Out]]
  ): Source[Out, Future[QueryResponse[Out]]] =
    queryResultsPages(jobId, startIndex, maxResults, timeout.map(_.toMillis).map(Math.toIntExact), location, None)
      .wireTapMat(Sink.head)(Keep.right)
      .mapConcat(_.rows.fold[List[Out]](Nil)(_.toList))

  private def queryResultsPages[Out](
      jobId: String,
      startIndex: Option[Long],
      maxResults: Option[Int],
      timeoutMs: Option[Int],
      location: Option[String],
      pageToken: Option[String]
  )(
      implicit um: FromEntityUnmarshaller[QueryResponse[Out]]
  ): Source[QueryResponse[Out], NotUsed] =
    source { settings =>
      val uri = BigQueryEndpoints.job(settings.projectId, jobId)
      val query = Query.Empty :+?
        "startIndex" -> startIndex :+?
        "maxResults" -> maxResults :+?
        "timeoutMs" -> timeoutMs :+?
        "location" -> location
      paginatedRequest[QueryResponse[Out]](HttpRequest(GET, uri.withQuery(query)), pageToken)
    }.mapMaterializedValue(_ => NotUsed)

  def insertAllAsync[In: ToEntityMarshaller](datasetId: String, tableId: String): Flow[In, Job, NotUsed] =
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

        val jobFlow = {
          val newline = ByteString("\n")
          val promise = Promise[Job]()
          val sink = Flow[In]
            .takeWithin(perTableQuota)
            .mapAsync(1)(Marshal(_).to[HttpEntity])
            .flatMapConcat(_.dataBytes)
            .intersperse(newline)
            .toMat(createLoadJob(job))(Keep.right)
            .mapMaterializedValue(promise.completeWith)
          Flow.fromSinkAndSource(sink, Source.future(promise.future))
        }

        Flow[In]
          .groupBy(1, _ => (), allowClosedSubstreamRecreation = true)
          .via(jobFlow)
          .concatSubstreams
      }
      .mapMaterializedValue(_ => NotUsed)

  def createLoadJob[Job: ToEntityMarshaller: FromEntityUnmarshaller](job: Job): Sink[ByteString, Future[Job]] =
    Sink
      .fromMaterializer { (mat, attr) =>
        import mat.executionContext
        implicit val settings = BigQueryAttributes.resolveSettings(attr, mat)
        val uri = BigQueryMediaEndpoints.jobs(settings.projectId)
        Sink
          .lazyFutureSink { () =>
            Marshal(job)
              .to[RequestEntity]
              .fast
              .map { entity =>
                LoadJob(HttpRequest(POST, uri, entity = entity))
              }(ExecutionContexts.parasitic)
          }
          .mapMaterializedValue(_.flatten)
      }
      .mapMaterializedValue(_.flatten)

}
