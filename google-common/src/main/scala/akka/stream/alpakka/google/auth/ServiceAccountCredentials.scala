/*
 * Copyright (C) since 2016 Lightbend Inc. <https://akka.io>
 */

package akka.stream.alpakka.google.auth

import akka.actor.ClassicActorSystemProvider
import akka.annotation.InternalApi
import akka.stream.Materializer
import akka.stream.alpakka.google.RequestSettings
import com.typesafe.config.Config
import spray.json.DefaultJsonProtocol._
import spray.json.{JsValue, JsonParser, RootJsonFormat}

import java.time.Clock
import scala.jdk.CollectionConverters._
import scala.concurrent.Future
import scala.io.Source

@InternalApi
private[alpakka] object ServiceAccountCredentials {

  def apply(projectId: Option[String], clientEmail: String, privateKey: String, scopes: Seq[String])(
      implicit system: ClassicActorSystemProvider
  ): Credentials =
    new ServiceAccountCredentials(projectId, clientEmail, privateKey, scopes)

  def apply(c: Config)(implicit system: ClassicActorSystemProvider): Credentials = {
    val scopes = c.getStringList("scopes").asScala.toSeq
    if (c.getString("private-key").nonEmpty) {
      val projectId = Some(c.getString("project-id")).filter(_.nonEmpty)
      val clientEmail = c.getString("client-email")
      val privateKey = c.getString("private-key")
      require(
        projectId.nonEmpty && clientEmail.nonEmpty && privateKey.nonEmpty && scopes.nonEmpty && scopes.forall(
          _.nonEmpty
        ),
        "Service account requires that project-id, client-email, private-key, and at least one scope are specified."
      )
      apply(projectId, clientEmail, privateKey, scopes)
    } else {
      val src = Source.fromFile(c.getString("path"))
      try {
        apply(JsonParser(src.mkString), scopes)
      } finally {
        src.close()
      }
    }
  }

  def apply(json: JsValue, scopes: Seq[String])(implicit system: ClassicActorSystemProvider): Credentials = {
    require(scopes.nonEmpty && scopes.forall(_.nonEmpty),
            "Service account requires that at least one scope is specified.")

    val credentials = json.convertTo[ServiceAccountCredentialsFile]
    ServiceAccountCredentials(credentials.project_id, credentials.client_email, credentials.private_key, scopes)
  }

  final case class ServiceAccountCredentialsFile(project_id: Option[String], client_email: String, private_key: String)
  implicit val serviceAccountCredentialsFormat: RootJsonFormat[ServiceAccountCredentialsFile] = jsonFormat3(
    ServiceAccountCredentialsFile
  )
}

@InternalApi
private final class ServiceAccountCredentials(projectId: Option[String],
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
