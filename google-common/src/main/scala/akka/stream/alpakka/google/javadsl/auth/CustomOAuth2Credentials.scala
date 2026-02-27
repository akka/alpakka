/*
 * Copyright (C) since 2016 Lightbend Inc. <https://akka.io>
 */

package akka.stream.alpakka.google.javadsl.auth

import akka.actor.ClassicActorSystemProvider
import akka.japi.Pair
import akka.stream.Materializer
import akka.stream.alpakka.google.auth.{Credentials, OAuth2Credentials}

import java.time.Instant
import java.util.concurrent.CompletionStage

import scala.jdk.FutureConverters._

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
   * @return A completion stage of the access token and its expiry time.
   */
  def retrieveAccessToken(): CompletionStage[Pair[String, Instant]]
}

object CustomOAuth2Credentials {

  /**
   * Create the credentials from the given custom credentials callback.
   */
  def create(credentials: CustomOAuth2Credentials, sys: ClassicActorSystemProvider): Credentials = {
    OAuth2Credentials.custom(() => {
      credentials.retrieveAccessToken().asScala.map(_.toScala)(sys.classicSystem.dispatcher)
    }, Materializer.matFromSystem(sys))
  }
}
