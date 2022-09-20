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
import scala.collection.JavaConverters._
import scala.concurrent.Future
import scala.io.Source

@InternalApi
private[alpakka] object ServiceAccountCredentials {

  def apply(projectId: String, clientEmail: String, privateKey: String, scopes: Seq[String])(
      implicit system: ClassicActorSystemProvider
  ): Credentials =
    new ServiceAccountCredentials(projectId, clientEmail, privateKey, scopes)

  def apply(c: Config)(implicit system: ClassicActorSystemProvider): Credentials = {
    val (projectId, clientEmail, privateKey) = {
      if (c.getString("private-key").nonEmpty) {
        (
          c.getString("project-id"),
          c.getString("client-email"),
          c.getString("private-key")
        )
      } else {
        val src = Source.fromFile(c.getString("path"))
        val credentials = JsonParser(src.mkString).convertTo[ServiceAccountCredentialsFile]
        src.close()
        (credentials.project_id, credentials.client_email, credentials.private_key)
      }
    }
    val scopes = c.getStringList("scopes").asScala.toSeq
    require(
      projectId.nonEmpty && clientEmail.nonEmpty && privateKey.nonEmpty && scopes.nonEmpty && scopes.forall(_.nonEmpty),
      "Service account requires that project-id, client-email, private-key, and at least one scope are specified."
    )
    apply(projectId, clientEmail, privateKey, scopes)
  }

  final case class ServiceAccountCredentialsFile(project_id: String, client_email: String, private_key: String)
  implicit val serviceAccountCredentialsFormat: RootJsonFormat[ServiceAccountCredentialsFile] = jsonFormat3(
    ServiceAccountCredentialsFile
  )
}

@InternalApi
private final class ServiceAccountCredentials(projectId: String,
                                              clientEmail: String,
                                              privateKey: String,
                                              scopes: Seq[String])(implicit mat: Materializer)
    extends OAuth2Credentials(projectId) {

  override protected def getAccessToken()(implicit mat: Materializer,
                                          settings: RequestSettings,
                                          clock: Clock): Future[AccessToken] = {
    GoogleOAuth2.getAccessToken(clientEmail, privateKey, scopes)
  }
}
