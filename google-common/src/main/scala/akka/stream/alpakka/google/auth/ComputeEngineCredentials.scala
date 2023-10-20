/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.google.auth

import akka.actor.ClassicActorSystemProvider
import akka.annotation.InternalApi
import akka.stream.Materializer
import akka.stream.alpakka.google.RequestSettings

import java.time.Clock
import scala.annotation.unused
import scala.concurrent.Future

@InternalApi
private[auth] object ComputeEngineCredentials {

  def apply()(implicit system: ClassicActorSystemProvider): Future[Credentials] =
    GoogleComputeMetadata
      .getProjectId()
      .map(new ComputeEngineCredentials(_))(system.classicSystem.dispatcher)

}

@InternalApi
private final class ComputeEngineCredentials(projectId: String)(implicit mat: Materializer)
    extends OAuth2Credentials(projectId) {
  override protected def getAccessToken()(implicit mat: Materializer,
                                          @unused settings: RequestSettings,
                                          clock: Clock): Future[AccessToken] =
    GoogleComputeMetadata.getAccessToken()
}
