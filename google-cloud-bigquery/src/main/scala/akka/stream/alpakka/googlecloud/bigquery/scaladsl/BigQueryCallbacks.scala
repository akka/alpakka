/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.bigquery.scaladsl

import akka.Done
import akka.actor.ClassicActorSystemProvider
import akka.annotation.ApiMayChange
import akka.dispatch.ExecutionContexts
import akka.http.scaladsl.model.Uri.Query
import akka.http.scaladsl.model.{HttpMethods, HttpRequest}
import akka.http.scaladsl.util.FastFuture
import akka.stream.alpakka.googlecloud.bigquery.impl.http.BigQueryHttp
import akka.stream.alpakka.googlecloud.bigquery.model.JobJsonProtocol.JobReference
import akka.stream.alpakka.googlecloud.bigquery.{BigQueryException, BigQuerySettings}

import scala.concurrent.Future

@ApiMayChange(issue = "https://github.com/akka/alpakka/issues/2353")
object BigQueryCallbacks {

  private val futureDone = FastFuture.successful(Done)

  val ignore: Any => Future[Done] = _ => futureDone

  def cancelJob(implicit system: ClassicActorSystemProvider,
                settings: BigQuerySettings): Option[JobReference] => Future[Done] = {
    case Some(JobReference(Some(projectId), Some(jobId), location)) =>
      import BigQueryException._
      val uri = BigQueryEndpoints.jobCancel(projectId, jobId)
      val query = Query(location.map("location" -> _).toMap)
      BigQueryHttp()
        .retryRequestWithOAuth(HttpRequest(HttpMethods.POST, uri.withQuery(query)))
        .flatMap(_.entity.discardBytes().future)(ExecutionContexts.parasitic)
    case _ => futureDone
  }
}
