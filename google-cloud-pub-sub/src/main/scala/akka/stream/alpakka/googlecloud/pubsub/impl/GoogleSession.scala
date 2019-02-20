/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.pubsub.impl

import akka.annotation.InternalApi
import akka.stream.Materializer
import akka.stream.alpakka.googlecloud.pubsub.impl.GoogleTokenApi.AccessTokenExpiry

import scala.concurrent.Future

@InternalApi
private[googlecloud] class GoogleSession(clientEmail: String, privateKey: String, tokenApi: GoogleTokenApi) {
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
