/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.bigquery.scaladsl

import akka.Done
import akka.actor.ClassicActorSystemProvider
import akka.dispatch.ExecutionContexts
import akka.http.scaladsl.util.FastFuture
import akka.stream.alpakka.googlecloud.bigquery.BigQuerySettings
import akka.stream.alpakka.googlecloud.bigquery.model.JobJsonProtocol.JobReference

import scala.concurrent.Future

object BigQueryCallbacks {

  private val futureDone = FastFuture.successful(Done)

  val ignore: Any => Future[Done] = _ => futureDone

  def cancelJob(implicit system: ClassicActorSystemProvider,
                settings: BigQuerySettings): Option[JobReference] => Future[Done] = {
    case Some(JobReference(Some(projectId), Some(jobId), location)) =>
      implicit val settingsWithProjectId = settings.copy(projectId = projectId)
      BigQuery.cancelJob(jobId, location)(system, settingsWithProjectId).map(_ => Done)(ExecutionContexts.parasitic)
    case _ => futureDone
  }
}
