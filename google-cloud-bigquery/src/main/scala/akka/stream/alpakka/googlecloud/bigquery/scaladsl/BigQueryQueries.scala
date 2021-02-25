/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.bigquery.scaladsl

import akka.NotUsed
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import akka.http.scaladsl.marshalling.Marshal
import akka.http.scaladsl.model.HttpMethods.{GET, POST}
import akka.http.scaladsl.model.Uri.Query
import akka.http.scaladsl.model.{HttpRequest, RequestEntity}
import akka.http.scaladsl.unmarshalling.{FromEntityUnmarshaller, Unmarshal}
import akka.stream.RestartSettings
import akka.stream.alpakka.googlecloud.bigquery.impl.http.BigQueryHttp
import akka.stream.alpakka.googlecloud.bigquery.model.JobJsonProtocol.JobReference
import akka.stream.alpakka.googlecloud.bigquery.model.QueryJsonProtocol.{QueryRequest, QueryResponse}
import akka.stream.alpakka.googlecloud.bigquery.{BigQueryAttributes, BigQueryEndpoints, BigQueryException}
import akka.stream.scaladsl.{Keep, RestartSource, Sink, Source}

import scala.concurrent.{Future, Promise}
import scala.concurrent.duration.FiniteDuration
import scala.util.{Failure, Success}

private[scaladsl] trait BigQueryQueries { this: BigQueryRest =>

  /**
   * Runs a BigQuery SQL query.
   * @see [[https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs/query BigQuery reference]]
   *
   * @param query a query string, following the BigQuery query syntax, of the query to execute
   * @param dryRun if set to `true` BigQuery doesn't run the job and instead returns statistics about the job such as how many bytes would be processed
   * @param useLegacySql specifies whether to use BigQuery's legacy SQL dialect for this query
   * @tparam Out the data model of the query results
   * @return a [[akka.stream.scaladsl.Source]] that emits an [[Out]] for each row of the result and materializes
   *         a [[scala.concurrent.Future]] containing the [[akka.stream.alpakka.googlecloud.bigquery.model.QueryJsonProtocol.QueryResponse]]
   */
  def query[Out](query: String, dryRun: Boolean = false, useLegacySql: Boolean = true)(
      implicit um: FromEntityUnmarshaller[QueryResponse[Out]]
  ): Source[Out, Future[QueryResponse[Out]]] = {
    val request = QueryRequest(query, None, None, None, Some(dryRun), Some(useLegacySql), None)
    this.query(request).mapMaterializedValue(_._2)
  }

  /**
   * Runs a BigQuery SQL query.
   * @see [[https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs/query BigQuery reference]]
   *
   * @param query the [[akka.stream.alpakka.googlecloud.bigquery.model.QueryJsonProtocol.QueryRequest]]
   * @tparam Out the data model of the query results
   * @return a [[akka.stream.scaladsl.Source]] that emits an [[Out]] for each row of the results and materializes
   *         a [[scala.concurrent.Future]] containing the [[akka.stream.alpakka.googlecloud.bigquery.model.JobJsonProtocol.JobReference]] and
   *         a [[scala.concurrent.Future]] containing the [[akka.stream.alpakka.googlecloud.bigquery.model.QueryJsonProtocol.QueryResponse]]
   */
  def query[Out](query: QueryRequest)(
      implicit um: FromEntityUnmarshaller[QueryResponse[Out]]
  ): Source[Out, (Future[JobReference], Future[QueryResponse[Out]])] =
    Source
      .fromMaterializer { (mat, attr) =>
        import BigQueryException._
        import SprayJsonSupport._
        import mat.executionContext
        implicit val system = mat.system
        implicit val settings = BigQueryAttributes.resolveSettings(attr, mat)

        val jobReference = Promise[JobReference]()

        Source
          .lazyFutureSource { () =>
            for {
              entity <- Marshal(query).to[RequestEntity]
              initialRequest = HttpRequest(POST, BigQueryEndpoints.queries(settings.projectId), entity = entity)
              response <- BigQueryHttp().retryRequestWithOAuth(initialRequest)
              initialQueryResponse <- Unmarshal(response.entity).to[QueryResponse[Out]]
            } yield {

              jobReference.success(initialQueryResponse.jobReference)

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
                    .addAttributes(attr)
                  val restartSettings = RestartSettings(minBackoff, maxBackoff, randomFactor)
                  RestartSource.onFailuresWithBackoff(restartSettings)(() => pages).map(_.get)
                }

              head.concat(tail)
            }

          }
          .mapMaterializedValue(_ => jobReference.future)
      }
      .mapMaterializedValue(_.flatten)
      .wireTapMat(Sink.head)(Keep.both)
      .mapConcat(_.rows.fold[List[Out]](Nil)(_.toList))

  /**
   * The results of a query job.
   * @see [[https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs/getQueryResults BigQuery reference]]
   *
   * @param jobId job ID of the query job
   * @param startIndex zero-based index of the starting row
   * @param maxResults maximum number of results to read
   * @param timeout specifies the maximum amount of time that the client is willing to wait for the query to complete
   * @param location the geographic location of the job. Required except for US and EU
   * @tparam Out the data model of the query results
   * @return a [[akka.stream.scaladsl.Source]] that emits an [[Out]] for each row of the results and materializes a [[scala.concurrent.Future]] containing the [[akka.stream.alpakka.googlecloud.bigquery.model.QueryJsonProtocol.QueryResponse]]
   */
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
      val uri = BigQueryEndpoints.query(settings.projectId, jobId)
      val query = ("startIndex" -> startIndex) ?+:
        ("maxResults" -> maxResults) ?+:
        ("timeoutMs" -> timeoutMs) ?+:
        ("location" -> location) ?+:
        Query.Empty
      paginatedRequest[QueryResponse[Out]](HttpRequest(GET, uri), query, pageToken)
    }.mapMaterializedValue(_ => NotUsed)

}
