/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.google.auth
import akka.annotation.InternalApi
import akka.http.scaladsl.model.headers.OAuth2BearerToken
import akka.stream.alpakka.google.RequestSettings
import com.google.auth.{Credentials => GoogleCredentials}
import com.typesafe.config.Config

import java.net.URI
import java.util
import scala.concurrent.{ExecutionContext, Future}

@InternalApi
private[alpakka] object NoCredentials {

  def apply(c: Config): NoCredentials = NoCredentials(c.getString("project-id"), c.getString("token"))

}

@InternalApi
private[auth] final case class NoCredentials private (projectId: String, token: String) extends Credentials {

  private val futureToken = Future.successful(OAuth2BearerToken(token))

  override def getToken()(implicit ec: ExecutionContext, settings: RequestSettings): Future[OAuth2BearerToken] =
    futureToken

  override def asGoogle(implicit ec: ExecutionContext, settings: RequestSettings): GoogleCredentials =
    new GoogleCredentials {
      override def getAuthenticationType: String = "<none>"
      override def getRequestMetadata(uri: URI): util.Map[String, util.List[String]] = util.Collections.emptyMap()
      override def hasRequestMetadata: Boolean = false
      override def hasRequestMetadataOnly: Boolean = true
      override def refresh(): Unit = ()
    }
}
