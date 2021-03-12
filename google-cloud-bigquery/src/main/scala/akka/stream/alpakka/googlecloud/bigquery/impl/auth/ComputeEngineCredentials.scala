/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.bigquery.impl.auth

import akka.actor.{ActorContext, ClassicActorSystemProvider, Props}
import akka.annotation.InternalApi
import akka.dispatch.ExecutionContexts
import akka.stream.Materializer
import akka.stream.alpakka.googlecloud.bigquery.BigQuerySettings

import scala.concurrent.Future

@InternalApi
private[bigquery] object ComputeEngineCredentials {

  def apply()(implicit system: ClassicActorSystemProvider): Future[CredentialsProvider] = {
    val credentials = system.classicSystem.actorOf(Props(new ComputeEngineCredentials))
    GoogleComputeMetadata
      .getProjectId()
      .map { projectId =>
        new OAuth2CredentialsProvider(projectId, credentials)
      }(ExecutionContexts.parasitic)
  }

}

@InternalApi
private final class ComputeEngineCredentials extends OAuth2Credentials {
  override protected def getAccessToken()(implicit ctx: ActorContext,
                                          settings: BigQuerySettings): Future[AccessToken] = {
    implicit val mat = Materializer(ctx)
    GoogleComputeMetadata.getAccessToken()
  }
}
