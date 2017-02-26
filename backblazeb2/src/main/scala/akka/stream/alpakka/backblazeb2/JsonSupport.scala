/*
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.backblazeb2

import akka.stream.alpakka.backblazeb2.Protocol.{AccountAuthorizationToken, AuthorizeAccountResponse}
import io.circe.{Decoder, Encoder}
import io.circe.generic.auto._

object JsonSupport {
  implicit val accountAuthorizationTokenEncoder: Encoder[AccountAuthorizationToken] =
    Encoder.encodeString.contramap(_.value)
  implicit val accountAuthorizationTokenDecoder: Decoder[AccountAuthorizationToken] =
    Decoder.decodeString.map(x => AccountAuthorizationToken(x))
  implicit val authorizeAccountResponseDecoder = Decoder[AuthorizeAccountResponse]
}
