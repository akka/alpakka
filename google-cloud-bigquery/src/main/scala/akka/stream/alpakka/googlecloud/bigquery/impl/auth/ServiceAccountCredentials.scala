/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.bigquery.impl.auth

import akka.actor.{ActorContext, ClassicActorSystemProvider, Props}
import akka.annotation.InternalApi
import akka.stream.Materializer
import akka.stream.alpakka.googlecloud.bigquery.BigQuerySettings
import com.typesafe.config.Config
import spray.json.DefaultJsonProtocol._
import spray.json.{JsonParser, RootJsonFormat}

import scala.collection.JavaConverters._
import scala.concurrent.Future
import scala.io.Source

@InternalApi
private[bigquery] object ServiceAccountCredentials {

  def apply(projectId: String, clientEmail: String, privateKey: String, scopes: Seq[String])(
      implicit system: ClassicActorSystemProvider
  ): CredentialsProvider = {
    val credentials =
      system.classicSystem.actorOf(Props(new ServiceAccountCredentials(clientEmail, privateKey, scopes)))
    new OAuth2CredentialsProvider(projectId, credentials)
  }

  def apply(c: Config)(implicit system: ClassicActorSystemProvider): CredentialsProvider = {
    val (projectId, clientEmail, privateKey) = {
      if (c.getString("path").isEmpty) {
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
    extends OAuth2Credentials {

  override protected def getAccessToken()(implicit ctx: ActorContext,
                                          settings: BigQuerySettings): Future[AccessToken] = {
    implicit val mat = Materializer(ctx)
    GoogleOAuth2.getAccessToken(clientEmail, privateKey, scopes)
  }
}
