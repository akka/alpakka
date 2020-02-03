/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.bigquery.impl

import akka.actor.ActorSystem
import akka.annotation.InternalApi
import akka.http.scaladsl.Http
import akka.stream.Materializer
import akka.stream.alpakka.googlecloud.bigquery.ForwardProxy
import akka.stream.alpakka.googlecloud.bigquery.impl.GoogleTokenApi.AccessTokenExpiry

import scala.concurrent.Future

@InternalApi
private[bigquery] object GoogleSession {
  def apply(clientEmail: String,
            privateKey: String,
            actorSystem: ActorSystem,
            forwardProxy: Option[ForwardProxy]): GoogleSession = {
    new GoogleSession(clientEmail, privateKey, new GoogleTokenApi(Http()(actorSystem), actorSystem, forwardProxy))
  }
}

@InternalApi
private[bigquery] class GoogleSession(clientEmail: String, privateKey: String, tokenApi: GoogleTokenApi) {
  protected var maybeAccessToken: Option[Future[AccessTokenExpiry]] = None

  private def getNewToken()(implicit materializer: Materializer): Future[AccessTokenExpiry] = {
    val accessToken = tokenApi.getAccessToken(clientEmail = clientEmail, privateKey = privateKey)
    maybeAccessToken = Some(accessToken)
    accessToken
  }

  private def expiresSoon(g: AccessTokenExpiry): Boolean =
    g.expiresAt < (tokenApi.now + 60)

  def getToken()(implicit materializer: Materializer): Future[String] = {
    import materializer.executionContext
    maybeAccessToken
      .getOrElse(getNewToken())
      .flatMap { result =>
        if (expiresSoon(result)) {
          getNewToken()
        } else {
          Future.successful(result)
        }
      }
      .map(_.accessToken)
  }
}
