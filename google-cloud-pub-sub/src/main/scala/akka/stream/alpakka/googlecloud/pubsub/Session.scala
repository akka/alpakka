/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.googlecloud.pubsub

import java.security.PrivateKey
import java.time.Instant

import akka.actor.ActorSystem
import akka.stream.Materializer

import scala.concurrent.Future

@akka.annotation.InternalApi
private[pubsub] class Session(clientEmail: String, privateKey: PrivateKey) {
  protected var maybeAccessToken: Option[Future[AccessTokenExpiry]] = None
  protected def now = Instant.now()
  protected val httpApi: HttpApi = HttpApi

  private def getNewToken()(implicit as: ActorSystem, materializer: Materializer): Future[AccessTokenExpiry] = {
    val accessToken = httpApi.getAccessToken(clientEmail = clientEmail, privateKey = privateKey, when = now)
    maybeAccessToken = Some(accessToken)
    accessToken
  }

  private def expiresSoon(g: AccessTokenExpiry): Boolean =
    g.expiresAt < (now.getEpochSecond + 60)

  def getToken()(implicit as: ActorSystem, materializer: Materializer): Future[String] = {
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
