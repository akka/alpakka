/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.google.auth

import akka.actor.{ActorContext, ClassicActorSystemProvider, Props}
import akka.annotation.InternalApi
import akka.stream.Materializer
import akka.stream.alpakka.google.GoogleSettings
import com.typesafe.config.Config
import spray.json.DefaultJsonProtocol._
import spray.json.{JsonParser, RootJsonFormat}

import scala.collection.JavaConverters._
import scala.concurrent.Future
import scala.io.Source

@InternalApi
private[auth] object ServiceAccountCredentials {

  def apply(projectId: String, clientEmail: String, privateKey: String, scopes: Seq[String])(
      implicit system: ClassicActorSystemProvider
  ): Credentials = {
    val credentials =
      system.classicSystem.actorOf(Props(new ServiceAccountCredentials(clientEmail, privateKey, scopes)))
    new OAuth2Credentials(projectId, credentials)
  }

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
    apply(projectId, clientEmail, privateKey, scopes)
  }

  final case class ServiceAccountCredentialsFile(project_id: String, client_email: String, private_key: String)
  implicit val serviceAccountCredentialsFormat: RootJsonFormat[ServiceAccountCredentialsFile] = jsonFormat3(
    ServiceAccountCredentialsFile
  )
}

@InternalApi
private final class ServiceAccountCredentials(clientEmail: String, privateKey: String, scopes: Seq[String])
    extends OAuth2CredentialsActor {

  override protected def getAccessToken()(implicit ctx: ActorContext, settings: GoogleSettings): Future[AccessToken] = {
    implicit val mat = Materializer(ctx)
    GoogleOAuth2.getAccessToken(clientEmail, privateKey, scopes)
  }
}
