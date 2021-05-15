/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.google.auth

import akka.actor.ClassicActorSystemProvider
import akka.annotation.InternalApi
import akka.stream.Materializer
import akka.stream.alpakka.google.{ComputeMetadata, RequestSettings}

import java.time.Clock
import scala.concurrent.Future

@InternalApi
private[auth] object ComputeEngineCredentials {

  def apply()(implicit system: ClassicActorSystemProvider): Future[Credentials] =
    ComputeMetadata.getProjectId
      .map(new ComputeEngineCredentials(_))(system.classicSystem.dispatcher)

}

@InternalApi
private final class ComputeEngineCredentials(projectId: String)(implicit mat: Materializer)
    extends OAuth2Credentials(projectId) {
  override protected def getAccessToken()(implicit mat: Materializer,
                                          settings: RequestSettings,
                                          clock: Clock): Future[AccessToken] =
    ComputeMetadata.getAccessToken()
}
