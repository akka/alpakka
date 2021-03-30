/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.huawei.pushkit.impl

import akka.actor.ActorSystem
import akka.annotation.InternalApi
import akka.http.scaladsl.HttpExt
import akka.http.scaladsl.model.{FormData, HttpMethods, HttpRequest}
import akka.http.scaladsl.unmarshalling.Unmarshal
import akka.stream.Materializer
import akka.stream.alpakka.huawei.pushkit.ForwardProxyHttpsContext.ForwardProxyHttpsContext
import akka.stream.alpakka.huawei.pushkit.ForwardProxyPoolSettings.ForwardProxyPoolSettings
import HmsTokenApi.{AccessTokenExpiry, OAuthResponse}
import akka.stream.alpakka.huawei.pushkit.ForwardProxy
import pdi.jwt.JwtTime

import java.time.Clock
import scala.concurrent.Future

/**
 * INTERNAL API
 */
@InternalApi
private[pushkit] class HmsTokenApi(http: => HttpExt, system: ActorSystem, forwardProxy: Option[ForwardProxy]) {
  import PushKitJsonSupport._

  private val authUrl = "https://oauth-login.cloud.huawei.com/oauth2/v3/token"

  def now: Long = JwtTime.nowSeconds(Clock.systemUTC())

  def getAccessToken(clientId: String, privateKey: String)(
      implicit materializer: Materializer
  ): Future[AccessTokenExpiry] = {
    import materializer.executionContext
    val expiresAt = now + 3600

    val requestEntity = FormData(
      "grant_type" -> "client_credentials",
      "client_secret" -> privateKey,
      "client_id" -> clientId
    ).toEntity

    for {
      response <- forwardProxy match {
        case Some(fp) =>
          http.singleRequest(HttpRequest(HttpMethods.POST, authUrl, entity = requestEntity),
                             connectionContext = fp.httpsContext(system),
                             settings = fp.poolSettings(system))
        case None => http.singleRequest(HttpRequest(HttpMethods.POST, authUrl, entity = requestEntity))
      }
      result <- Unmarshal(response.entity).to[OAuthResponse]
    } yield {
      AccessTokenExpiry(
        accessToken = result.access_token,
        expiresAt = expiresAt
      )
    }
  }
}

/**
 * INTERNAL API
 */
@InternalApi
private[pushkit] object HmsTokenApi {
  case class AccessTokenExpiry(accessToken: String, expiresAt: Long)
  case class OAuthResponse(access_token: String, token_type: String, expires_in: Int)
}
