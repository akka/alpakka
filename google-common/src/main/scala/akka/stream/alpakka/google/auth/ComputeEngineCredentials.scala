/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.google.auth

import akka.actor.{ActorContext, ClassicActorSystemProvider, Props}
import akka.annotation.InternalApi
import akka.dispatch.ExecutionContexts
import akka.stream.Materializer
import akka.stream.alpakka.google.GoogleSettings

import scala.concurrent.Future

@InternalApi
private[auth] object ComputeEngineCredentials {

  def apply()(implicit system: ClassicActorSystemProvider): Future[Credentials] = {
    val credentials = system.classicSystem.actorOf(Props(new ComputeEngineCredentials))
    GoogleComputeMetadata
      .getProjectId()
      .map { projectId =>
        new OAuth2Credentials(projectId, credentials)
      }(ExecutionContexts.parasitic)
  }

}

@InternalApi
private final class ComputeEngineCredentials extends OAuth2CredentialsActor {
  override protected def getAccessToken()(implicit ctx: ActorContext, settings: GoogleSettings): Future[AccessToken] = {
    implicit val mat = Materializer(ctx)
    GoogleComputeMetadata.getAccessToken()
  }
}
