/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.backblazeb2.scaladsl

import akka.stream.ActorMaterializer
import akka.stream.alpakka.backblazeb2.Protocol.{AuthorizeAccountResponse, B2AccountCredentials, B2Response}
import cats.data.EitherT
import RetryUtils._
import scala.concurrent.Promise

class B2AccountAuthorizer(api: B2API, accountCredentials: B2AccountCredentials, eager: Boolean = false)(
    implicit materializer: ActorMaterializer
) {
  implicit val executionContext = materializer.executionContext

  @volatile private var authorizeAccountPromise: Promise[AuthorizeAccountResponse] = Promise()

  if (eager) { // if authorization is eager, let us start with this upon construction
    val _ = obtainAuthorizeAccountResponse()
  }

  def withAuthorization[T](f: AuthorizeAccountResponse => B2Response[T]): B2Response[T] = {
    import cats.implicits._
    val result = for {
      authorizeAccountResponse <- EitherT(obtainAuthorizeAccountResponse())
      operation <- EitherT(f(authorizeAccountResponse))
    } yield operation

    tryAgainIfExpired(result.value) {
      authorizeAccountPromise = Promise()
      withAuthorization(f)
    }
  }

  private def callAuthorizeAccount(): B2Response[AuthorizeAccountResponse] =
    api.authorizeAccount(accountCredentials)

  /**
   * Return the saved AuthorizeAccountResponse if it exists or obtain a new one if it doesn't
   */
  private def obtainAuthorizeAccountResponse(): B2Response[AuthorizeAccountResponse] =
    returnOrObtain(authorizeAccountPromise, callAuthorizeAccount _)
}
