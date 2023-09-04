/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.google.auth

import akka.annotation.InternalApi
import akka.stream.alpakka.google.RequestSettings
import com.google.auth.{RequestMetadataCallback, Credentials => GoogleCredentials}

import java.net.URI
import java.util
import java.util.concurrent.Executor
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, ExecutionContextExecutor, Future}
import scala.util.{Failure, Success}

@InternalApi
private[auth] final class GoogleOAuth2Credentials(credentials: OAuth2Credentials)(
    implicit ec: ExecutionContext,
    settings: RequestSettings
) extends GoogleCredentials {

  override def getAuthenticationType: String = "OAuth2"
  override def hasRequestMetadata: Boolean = true
  override def hasRequestMetadataOnly: Boolean = true

  override def getRequestMetadata(uri: URI): util.Map[String, util.List[String]] =
    Await.result(requestMetadata, Duration.Inf)

  override def getRequestMetadata(uri: URI, executor: Executor, callback: RequestMetadataCallback): Unit = {
    implicit val ec: ExecutionContextExecutor = ExecutionContext.fromExecutor(executor)
    requestMetadata.onComplete {
      case Success(metadata) => callback.onSuccess(metadata)
      case Failure(ex) => callback.onFailure(ex)
    }
  }

  private def requestMetadata(implicit ec: ExecutionContext): Future[util.Map[String, util.List[String]]] = {
    credentials.get().map { token =>
      util.Collections.singletonMap("Authorization", util.Collections.singletonList(token.toString))
    }
  }

  override def refresh(): Unit = credentials.refresh()
}
