/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.google.auth
import akka.annotation.InternalApi
import akka.http.scaladsl.model.headers.OAuth2BearerToken
import akka.stream.alpakka.google.GoogleSettings
import com.google.auth.{Credentials => GoogleCredentials}

import java.net.URI
import java.util
import scala.concurrent.{ExecutionContext, Future}

@InternalApi
private[alpakka] object NoCredentials extends Credentials {

  private val token = Future.successful(OAuth2BearerToken("<no-token>"))

  override def projectId: String = "<no-project-id>"

  override def getToken()(implicit ec: ExecutionContext, settings: GoogleSettings): Future[OAuth2BearerToken] = token

  override def asGoogle(implicit ec: ExecutionContext, settings: GoogleSettings): GoogleCredentials =
    new GoogleCredentials {
      override def getAuthenticationType: String = "<none>"
      override def getRequestMetadata(uri: URI): util.Map[String, util.List[String]] = util.Collections.emptyMap()
      override def hasRequestMetadata: Boolean = false
      override def hasRequestMetadataOnly: Boolean = true
      override def refresh(): Unit = ()
    }

}
