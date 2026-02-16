/*
 * Copyright (C) since 2016 Lightbend Inc. <https://akka.io>
 */

package akka.stream.alpakka.google.scaladsl.auth

import akka.actor.ClassicActorSystemProvider
import akka.stream.Materializer
import akka.stream.alpakka.google.auth.{Credentials, OAuth2Credentials}

import java.time.Instant
import scala.concurrent.Future

/**
 * This class is intended for users to implement themselves to provide a custom means of loading Google OAuth2
 * credentials.
 */
trait CustomOAuth2Credentials {

  /**
   * Retrieve an access token.
   *
   * This will be called whenever the previous access token has expired.
   *
   * @return A future of the access token and its expiry time.
   */
  def retrieveAccessToken(): Future[(String, Instant)]
}

object CustomOAuth2Credentials {

  def apply(credentials: CustomOAuth2Credentials)(implicit sys: ClassicActorSystemProvider): Credentials = {
    OAuth2Credentials.custom(credentials.retrieveAccessToken _, Materializer.matFromSystem)
  }
}
