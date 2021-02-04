/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.bigquery.scaladsl

import akka.NotUsed
import akka.actor.ClassicActorSystemProvider
import akka.dispatch.ExecutionContexts
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import akka.http.scaladsl.marshalling.{Marshal, ToEntityMarshaller}
import akka.http.scaladsl.model.HttpMethods.{GET, POST}
import akka.http.scaladsl.model.Uri.Query
import akka.http.scaladsl.model.{HttpEntity, HttpRequest, RequestEntity}
import akka.http.scaladsl.unmarshalling.{FromEntityUnmarshaller, Unmarshal}
import akka.http.scaladsl.util.FastFuture.EnhancedFuture
import akka.stream.alpakka.googlecloud.bigquery.impl.LoadJob
import akka.stream.alpakka.googlecloud.bigquery.impl.http.BigQueryHttp
import akka.stream.alpakka.googlecloud.bigquery.model.JobJsonProtocol.{
  CreateNeverDisposition,
  Job,
  JobCancelResponse,
  JobConfiguration,
  JobConfigurationLoad,
  NewlineDelimitedJsonFormat,
  WriteAppendDisposition
}
import akka.stream.alpakka.googlecloud.bigquery.model.TableJsonProtocol.TableReference
import akka.stream.alpakka.googlecloud.bigquery._
import akka.stream.scaladsl.{Flow, Keep, Sink, Source}
import akka.util.ByteString
import com.github.ghik.silencer.silent

import scala.concurrent.{Future, Promise}

private[scaladsl] trait BigQueryJobs { this: BigQueryRest =>

  /**
   * Returns information about a specific job.
   * @see [[https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs/get BigQuery reference]]
   *
   * @param jobId job ID of the requested job
   * @param location the geographic location of the job. Required except for US and EU
   * @return a [[scala.concurrent.Future]] containing the [[akka.stream.alpakka.googlecloud.bigquery.model.JobJsonProtocol.Job]]
   */
  def job(jobId: String, location: Option[String] = None)(implicit system: ClassicActorSystemProvider,
                                                          settings: BigQuerySettings): Future[Job] = {
    import BigQueryException._
    import SprayJsonSupport._
    val uri = BigQueryEndpoints.job(settings.projectId, jobId)
    val query = ("location" -> location) ?+: Query.Empty
    BigQueryHttp()
      .retryRequestWithOAuth(HttpRequest(GET, uri.withQuery(query)))
      .flatMap { response =>
        Unmarshal(response.entity).to[Job]
      }(system.classicSystem.dispatcher)
  }

  /**
   * Requests that a job be cancelled.
   * @see [[https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs/cancel BigQuery reference]]
   *
   * @param jobId job ID of the job to cancel
   * @param location the geographic location of the job. Required except for US and EU
   * @return a [[scala.concurrent.Future]] containing the [[akka.stream.alpakka.googlecloud.bigquery.model.JobJsonProtocol.JobCancelResponse]]
   */
  def cancelJob(
      jobId: String,
      location: Option[String] = None
  )(implicit system: ClassicActorSystemProvider, settings: BigQuerySettings): Future[JobCancelResponse] = {
    import BigQueryException._
    import SprayJsonSupport._
    implicit val ec = system.classicSystem.dispatcher
    val uri = BigQueryEndpoints.jobCancel(settings.projectId, jobId)
    val query = ("location" -> location) ?+: Query.Empty
    BigQueryHttp()
      .retryRequestWithOAuth(HttpRequest(POST, uri.withQuery(query)))
      .flatMap { response =>
        Unmarshal(response.entity).to[JobCancelResponse]
      }(system.classicSystem.dispatcher)
  }

  /**
   * Loads data into BigQuery via a series of asynchronous load jobs, configurable by [[akka.stream.alpakka.googlecloud.bigquery.LoadJobSettings]].
   * @note WARNING: Pending the resolution of [[ https://issuetracker.google.com/176002651 BigQuery issue 176002651]] this method may not work as expected.
   *
   * @param datasetId dataset ID of the table to insert into
   * @param tableId table ID of the table to insert into
   * @tparam In the data model for each record
   * @return a [[akka.stream.scaladsl.Flow]] that uploads each [[In]] and emits a [[akka.stream.alpakka.googlecloud.bigquery.model.JobJsonProtocol.Job]] for every upload job created
   */
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

  /**
   * Starts a new asynchronous upload job.
   * @note WARNING: Pending the resolution of [[https://issuetracker.google.com/176002651 BigQuery issue 176002651]] this method may not work as expected.
   * @see [[https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs/insert BigQuery reference]]
   * @see [[https://cloud.google.com/bigquery/docs/reference/api-uploads BigQuery reference]]
   *
   * @param job the job to start
   * @tparam Job the data model for a job
   * @return a [[akka.stream.scaladsl.Sink]] that uploads bytes and materializes a [[scala.concurrent.Future]] containing the [[Job]] when completed
   */
  def createLoadJob[@silent("shadows") Job: ToEntityMarshaller: FromEntityUnmarshaller](
      job: Job
  ): Sink[ByteString, Future[Job]] =
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
