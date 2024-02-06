/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.google.auth

import akka.actor.ClassicActorSystemProvider
import akka.annotation.InternalApi
import akka.stream.Materializer
import akka.stream.alpakka.google.RequestSettings
import com.typesafe.config.Config
import spray.json.DefaultJsonProtocol._
import spray.json.{JsonParser, RootJsonFormat}

import java.time.Clock
import scala.annotation.unused
import scala.concurrent.Future
import scala.io.Source

@InternalApi
private[alpakka] object UserAccessCredentials {

  def apply(clientId: String, clientSecret: String, refreshToken: String, projectId: String)(implicit
      system: ClassicActorSystemProvider
  ): Credentials = {
    require(
      clientId.nonEmpty && clientSecret.nonEmpty && refreshToken.nonEmpty && projectId.nonEmpty,
      "User access credentials requires that client id, client secret, refresh token, and project id are defined."
    )
    new UserAccessCredentials(clientId, clientSecret, refreshToken, projectId)
  }

  def apply(c: Config)(implicit system: ClassicActorSystemProvider): Credentials = {
    if (c.getString("client-id").nonEmpty) {
      apply(
        clientId = c.getString("client-id"),
        clientSecret = c.getString("client-secret"),
        refreshToken = c.getString("refresh-token"),
        projectId = c.getString("project-id")
      )
    } else {
      val src = Source.fromFile(c.getString("path"))
      val credentials = JsonParser(src.mkString).convertTo[UserAccessCredentialsFile]
      src.close()
      apply(
        clientId = credentials.client_id,
        clientSecret = credentials.client_secret,
        refreshToken = credentials.refresh_token,
        projectId = credentials.quota_project_id
      )
    }
  }

  final case class UserAccessCredentialsFile(client_id: String,
                                             client_secret: String,
                                             refresh_token: String,
                                             quota_project_id: String
  )
  implicit val userAccessCredentialsFormat: RootJsonFormat[UserAccessCredentialsFile] = jsonFormat4(
    UserAccessCredentialsFile
  )
}

@InternalApi
private final class UserAccessCredentials(clientId: String,
                                          clientSecret: String,
                                          refreshToken: String,
                                          projectId: String
)(implicit
    mat: Materializer
) extends OAuth2Credentials(projectId) {

  override protected def getAccessToken()(implicit
      mat: Materializer,
      @unused settings: RequestSettings,
      clock: Clock
  ): Future[AccessToken] = {
    UserAccessMetadata.getAccessToken(clientId, clientSecret, refreshToken)
  }
}
