/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.bigquery.scaladsl
import akka.NotUsed
import akka.actor.ActorSystem
import akka.annotation.ApiMayChange
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.{HttpMethods, HttpRequest}
import akka.stream.alpakka.googlecloud.bigquery.BigQueryConfig
import akka.stream.alpakka.googlecloud.bigquery.client.GoogleEndpoints
import akka.stream.alpakka.googlecloud.bigquery.impl.parser.Parser.PagingInfo
import akka.stream.alpakka.googlecloud.bigquery.impl.sendrequest.SendRequestWithOauthHandling
import akka.stream.scaladsl.{Sink, Source}

@ApiMayChange(issue = "https://github.com/akka/alpakka/issues/2353")
object BigQueryCallbacks {
  val ignore: PagingInfo => NotUsed = (_: PagingInfo) => NotUsed
  def tryToStopJob(
      projectConfig: BigQueryConfig
  )(implicit actorSystem: ActorSystem): PagingInfo => NotUsed = { pageInfo: PagingInfo =>
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
