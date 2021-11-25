/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.huawei.pushkit.impl

import akka.annotation.InternalApi
import akka.stream.Materializer
import akka.stream.alpakka.huawei.pushkit.HmsSettings
import akka.stream.alpakka.huawei.pushkit.impl.HmsTokenApi.AccessTokenExpiry

import scala.concurrent.Future

/**
 * INTERNAL API
 */
@InternalApi
private class HmsSession(conf: HmsSettings, tokenApi: HmsTokenApi) {
  protected var maybeAccessToken: Option[Future[AccessTokenExpiry]] = None

  private def getNewToken()(implicit materializer: Materializer): Future[AccessTokenExpiry] = {
    val accessToken = tokenApi.getAccessToken(clientId = conf.appId, privateKey = conf.appSecret)
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
