/*
 * Copyright (C) since 2016 Lightbend Inc. <https://akka.io>
 */

package akka.stream.alpakka.google.auth

import akka.annotation.InternalApi
import akka.http.scaladsl.model.headers.OAuth2BearerToken
import akka.stream.alpakka.google.RequestSettings
import akka.stream.scaladsl.{Sink, Source}
import akka.stream.{CompletionStrategy, Materializer, OverflowStrategy}
import com.google.auth.{Credentials => GoogleCredentials}

import java.time.{Clock, Instant}
import scala.annotation.unused
import scala.concurrent.{ExecutionContext, Future, Promise}

@InternalApi
private[google] object OAuth2Credentials {
  sealed abstract class Command
  final case class TokenRequest(promise: Promise[OAuth2BearerToken], settings: RequestSettings) extends Command
  final case object ForceRefresh extends Command
  final case object Close extends Command

  def custom(retrieveAccessToken: () => Future[(String, Instant)], mat: Materializer): Credentials = {
    new OAuth2Credentials(None)(mat) {
      override protected def getAccessToken()(implicit mat: Materializer,
                                              settings: RequestSettings,
                                              clock: Clock): Future[AccessToken] = {
        retrieveAccessToken()
          .map { token =>
            AccessToken(token._1, token._2.getEpochSecond)
          }(mat.executionContext)
      }
    }
  }
}

@InternalApi
private[auth] abstract class OAuth2Credentials(val projectId: Option[String])(implicit mat: Materializer)
    extends Credentials {

  import OAuth2Credentials._

  private val tokenStream = stream.run()

  override def get()(implicit @unused ec: ExecutionContext, settings: RequestSettings): Future[OAuth2BearerToken] = {
    val token = Promise[OAuth2BearerToken]()
    tokenStream ! TokenRequest(token, settings)
    token.future
  }

  def refresh(): Unit = tokenStream ! ForceRefresh

  override def asGoogle(implicit ec: ExecutionContext, settings: RequestSettings): GoogleCredentials =
    new GoogleOAuth2Credentials(this)(ec, settings)

  protected def getAccessToken()(implicit mat: Materializer,
                                 settings: RequestSettings,
                                 clock: Clock): Future[AccessToken]

  private def stream =
    Source
      .actorRef[OAuth2Credentials.Command](
        {
          case Close => CompletionStrategy.draining
        }: PartialFunction[Any, CompletionStrategy],
        PartialFunction.empty[Any, Throwable],
        Int.MaxValue,
        OverflowStrategy.fail
      )
      .to(
        Sink.fromMaterializer { (mat, attr) =>
          Sink.foldAsync(Option.empty[AccessToken]) {
            case (cachedToken @ Some(token), TokenRequest(promise, _)) if !token.expiresSoon()(Clock.systemUTC()) =>
              promise.success(OAuth2BearerToken(token.token))
              Future.successful(cachedToken)
            case (_, TokenRequest(promise, settings)) =>
              getAccessToken()(mat, settings, Clock.systemUTC())
                .andThen {
                  case response =>
                    promise.complete(response.map(t => OAuth2BearerToken(t.token)))
                }(ExecutionContext.parasitic)
                .map(Some(_))(ExecutionContext.parasitic)
                .recover { case _ => None }(ExecutionContext.parasitic)
            case (_, ForceRefresh) =>
              Future.successful(None)
            case (token, Close) =>
              Future.successful(token)
          }
        }
      )

  override def close(): Unit = {
    tokenStream ! Close
  }
}
