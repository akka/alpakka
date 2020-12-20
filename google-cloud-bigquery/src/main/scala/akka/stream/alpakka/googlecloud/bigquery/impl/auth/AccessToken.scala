/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.bigquery.impl.auth

import akka.annotation.InternalApi
import akka.http.scaladsl.unmarshalling.Unmarshaller
import pdi.jwt.JwtTime
import spray.json.RootJsonFormat

import java.time.Clock
import scala.concurrent.duration._

@InternalApi
private[auth] final case class AccessToken(token: String, expiresAt: Long) {
  def expiresSoon(in: FiniteDuration = 1.minute)(implicit clock: Clock): Boolean =
    expiresAt < JwtTime.nowSeconds + in.toSeconds
}

@InternalApi
private[auth] object AccessToken {
  implicit def unmarshaller[T](implicit unmarshaller: Unmarshaller[T, AccessTokenResponse],
                               clock: Clock): Unmarshaller[T, AccessToken] =
    unmarshaller.map {
      case AccessTokenResponse(access_token, _, expires_in) =>
        AccessToken(access_token, JwtTime.now + expires_in)
    }
}

@InternalApi
private[auth] final case class AccessTokenResponse(access_token: String, token_type: String, expires_in: Int)

@InternalApi
private[auth] object AccessTokenResponse {
  import spray.json.DefaultJsonProtocol._
  implicit val format: RootJsonFormat[AccessTokenResponse] = jsonFormat3(AccessTokenResponse.apply)
}
