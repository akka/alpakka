/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.bigquery.scaladsl
import akka.NotUsed
import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.{HttpMethods, HttpRequest}
import akka.stream.Materializer
import akka.stream.alpakka.googlecloud.bigquery.BigQueryConfig
import akka.stream.alpakka.googlecloud.bigquery.client.GoogleEndpoints
import akka.stream.alpakka.googlecloud.bigquery.impl.parser.Parser.PagingInfo
import akka.stream.alpakka.googlecloud.bigquery.impl.sendrequest.SendRequestWithOauthHandling
import akka.stream.scaladsl.{Sink, Source}

object BigQueryCallbacks {
  val ignore: PagingInfo => NotUsed = (_: PagingInfo) => NotUsed
  def tryToStopJob(
      projectConfig: BigQueryConfig
  )(implicit actorSystem: ActorSystem, materializer: Materializer): PagingInfo => NotUsed = { pageInfo: PagingInfo =>
    {
      pageInfo.jobId.foreach(jobId => {
        Source
          .single(HttpRequest(HttpMethods.POST, GoogleEndpoints.cancellationUrl(projectConfig.projectId, jobId)))
          .via(SendRequestWithOauthHandling(projectConfig, Http()))
          .runWith(Sink.ignore)
      })
      NotUsed
    }
  }
}
