/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.google.http

import akka.actor.ClassicActorSystemProvider
import akka.annotation.InternalApi
import akka.dispatch.ExecutionContexts
import akka.http.scaladsl.model.headers.Authorization
import akka.http.scaladsl.model.{HttpRequest, HttpResponse}
import akka.http.scaladsl.unmarshalling.{FromResponseUnmarshaller, Unmarshal}
import akka.http.scaladsl.{Http, HttpExt}
import akka.stream.alpakka.google.GoogleSettings
import akka.stream.alpakka.google.util.Retry

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

  /**
   * Sends a single [[HttpRequest]] and returns the raw [[HttpResponse]].
   */
  def singleRawRequest(request: HttpRequest)(implicit settings: GoogleSettings): Future[HttpResponse] = {
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

  /**
   * Sends a single [[HttpRequest]] and returns the [[Unmarshal]]ed response.
   * Retries the request if the [[FromResponseUnmarshaller]] throws a [[akka.stream.alpakka.google.util.Retry]].
   */
  def singleRequest[T](request: HttpRequest)(
      implicit settings: GoogleSettings,
      um: FromResponseUnmarshaller[T]
  ): Future[T] = Retry(settings.retrySettings) {
    singleRawRequest(request).flatMap(Unmarshal(_).to[T])(ExecutionContexts.parasitic)
  }

  /**
   * Adds an Authentication header and sends a single [[HttpRequest]] and returns the [[Unmarshal]]ed response.
   * Retries the request if the [[FromResponseUnmarshaller]] throws a [[akka.stream.alpakka.google.util.Retry]].
   */
  def singleAuthenticatedRequest[T](request: HttpRequest)(
      implicit settings: GoogleSettings,
      um: FromResponseUnmarshaller[T]
  ): Future[T] = Retry(settings.retrySettings) {
    addOAuth(request).flatMap(singleRequest(_))(ExecutionContexts.parasitic)
  }

  private[http] def addOAuth(request: HttpRequest)(implicit settings: GoogleSettings): Future[HttpRequest] = {
    settings.credentials
      .getToken()
      .map { token =>
        request.addHeader(Authorization(token))
      }(ExecutionContexts.parasitic)
  }
}
