/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.google.http

import akka.actor.ClassicActorSystemProvider
import akka.annotation.InternalApi
import akka.dispatch.ExecutionContexts
import akka.http.scaladsl.model.StatusCodes.{ClientError, ServerError}
import akka.http.scaladsl.model.headers.Authorization
import akka.http.scaladsl.model.{HttpRequest, HttpResponse}
import akka.http.scaladsl.unmarshalling.{FromResponseUnmarshaller, Unmarshal}
import akka.http.scaladsl.util.FastFuture
import akka.http.scaladsl.{Http, HttpExt}
import akka.stream.alpakka.google.util.Retry
import akka.stream.alpakka.google.util.Retry.DoNotRetry
import akka.stream.alpakka.google.{GoogleSettings, RetrySettings}

import scala.concurrent.Future

@InternalApi
private[alpakka] object GoogleHttp {

  def apply()(implicit system: ClassicActorSystemProvider): GoogleHttp =
    new GoogleHttp(Http()(system.classicSystem))

  def apply(http: HttpExt): GoogleHttp = new GoogleHttp(http)

}

@InternalApi
private[alpakka] final class GoogleHttp private (val http: HttpExt) extends AnyVal {

  private implicit def system = http.system
  private implicit def ec = system.dispatcher
  private implicit def scheduler = system.scheduler

  def singleRequest(request: HttpRequest)(implicit settings: GoogleSettings): Future[HttpResponse] = {
    val requestWithStandardParams = request.withUri(
      request.uri.copy(
        rawQueryString = Some(
          request.uri.rawQueryString
            .fold(settings.requestSettings.queryString)(_.concat(settings.requestSettings.`&queryString`))
        )
      )
    )
    settings.forwardProxy.fold(http.singleRequest(requestWithStandardParams)) { proxy =>
      http.singleRequest(requestWithStandardParams, proxy.connectionContext, proxy.poolSettings)
    }
  }

  def singleRequestOrFail(request: HttpRequest)(
      implicit settings: GoogleSettings,
      exceptionUnmarshaller: FromResponseUnmarshaller[Exception]
  ): Future[HttpResponse] =
    singleRequest(request).flatMap(processResponse)

  def singleRequestWithOAuth(request: HttpRequest)(
      implicit settings: GoogleSettings
  ): Future[HttpResponse] =
    addOAuth(request).flatMap(singleRequest)(ExecutionContexts.parasitic)

  def singleRequestWithOAuthOrFail(request: HttpRequest)(
      implicit settings: GoogleSettings,
      exceptionUnmarshaller: FromResponseUnmarshaller[Exception]
  ): Future[HttpResponse] =
    addOAuth(request).flatMap(singleRequestOrFail)(ExecutionContexts.parasitic)

  def retryRequest(request: HttpRequest)(
      implicit settings: GoogleSettings,
      exceptionUnmarshaller: FromResponseUnmarshaller[Exception]
  ): Future[HttpResponse] = retry(settings.retrySettings)(singleRequest(request))

  def retryRequestWithOAuth(request: HttpRequest)(
      implicit settings: GoogleSettings,
      exceptionUnmarshaller: FromResponseUnmarshaller[Exception]
  ): Future[HttpResponse] =
    retry(settings.retrySettings) {
      addOAuth(request)
        .transform(identity, DoNotRetry)(ExecutionContexts.parasitic)
        .flatMap(singleRequest)(ExecutionContexts.parasitic)
    }

  private[http] def addOAuth(request: HttpRequest)(implicit settings: GoogleSettings): Future[HttpRequest] = {
    settings.credentials
      .getToken()
      .map { token =>
        request.addHeader(Authorization(token))
      }(ExecutionContexts.parasitic)
  }

  private def retry(retrySettings: RetrySettings)(request: => Future[HttpResponse])(
      implicit exceptionUnmarshaller: FromResponseUnmarshaller[Exception]
  ): Future[HttpResponse] =
    Retry(retrySettings) {
      request.flatMap { response =>
        processResponse(response).transform(
          identity,
          ex => {
            response.status match {
              // Retriable, see https://github.com/googleapis/google-api-python-client/blob/master/googleapiclient/http.py#L87
              // Unfortunately 403 encompasses many errors besides rateLimitExceeded and userRateLimitExceeded
              case ClientError(403) if ex.getMessage.contains("ateLimitExceeded") => ex
              case ClientError(429) => ex
              case ServerError(_) => ex
              case _ => DoNotRetry(ex)
            }
          }
        )(ExecutionContexts.parasitic)
      }(ExecutionContexts.parasitic)
    }

  private def processResponse(
      response: HttpResponse
  )(implicit um: FromResponseUnmarshaller[Exception]): Future[HttpResponse] =
    if (response.status.isFailure)
      Unmarshal(response).to[Exception].flatMap(FastFuture.failed)(ExecutionContexts.parasitic)
    else
      FastFuture.successful(response)
}
