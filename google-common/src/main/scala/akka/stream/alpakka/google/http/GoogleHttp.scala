/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.google.http

import akka.actor.ClassicActorSystemProvider
import akka.annotation.InternalApi
import akka.dispatch.ExecutionContexts
import akka.http.scaladsl.Http.HostConnectionPool
import akka.http.scaladsl.model.headers.Authorization
import akka.http.scaladsl.model.{HttpRequest, HttpResponse}
import akka.http.scaladsl.unmarshalling.{FromResponseUnmarshaller, Unmarshal}
import akka.http.scaladsl.{Http, HttpExt}
import akka.stream.alpakka.google.{GoogleAttributes, GoogleSettings, RequestSettings, RetrySettings}
import akka.stream.alpakka.google.util.Retry
import akka.stream.scaladsl.{Flow, FlowWithContext, Keep, RetryFlow}

import scala.concurrent.Future
import scala.util.{Failure, Success, Try}

@InternalApi
private[alpakka] object GoogleHttp {

  def apply()(implicit system: ClassicActorSystemProvider): GoogleHttp =
    new GoogleHttp(Http()(system.classicSystem))

  def apply(system: ClassicActorSystemProvider)(implicit dummy: DummyImplicit): GoogleHttp =
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
  def singleRawRequest(request: HttpRequest)(implicit settings: RequestSettings): Future[HttpResponse] = {
    val requestWithStandardParams = addStandardQuery(request)
    settings.forwardProxy.fold(http.singleRequest(requestWithStandardParams)) { proxy =>
      http.singleRequest(requestWithStandardParams, proxy.connectionContext, proxy.poolSettings)
    }
  }

  /**
   * Sends a single [[HttpRequest]] and returns the [[Unmarshal]]led response.
   * Retries the request if the [[FromResponseUnmarshaller]] throws a [[akka.stream.alpakka.google.util.Retry]].
   */
  def singleRequest[T](request: HttpRequest)(
      implicit settings: RequestSettings,
      um: FromResponseUnmarshaller[T]
  ): Future[T] = Retry(settings.retrySettings) {
    singleRawRequest(request).flatMap(Unmarshal(_).to[T])(ExecutionContexts.parasitic)
  }

  /**
   * Adds an Authorization header and sends a single [[HttpRequest]] and returns the [[Unmarshal]]led response.
   * Retries the request if the [[FromResponseUnmarshaller]] throws a [[akka.stream.alpakka.google.util.Retry]].
   */
  def singleAuthenticatedRequest[T](request: HttpRequest)(
      implicit settings: GoogleSettings,
      um: FromResponseUnmarshaller[T]
  ): Future[T] = Retry(settings.requestSettings.retrySettings) {
    implicit val requestSettings = settings.requestSettings
    addAuth(request).flatMap(singleRequest(_))(ExecutionContexts.parasitic)
  }

  /**
   * Creates a cached host connection pool that sends requests and emits the [[Unmarshal]]led response.
   * If `authenticate = true` adds an Authorization header to each request.
   * Retries the request if the [[FromResponseUnmarshaller]] throws a [[akka.stream.alpakka.google.util.Retry]].
   */
  def cachedHostConnectionPool[T: FromResponseUnmarshaller](
      host: String,
      port: Int = -1,
      https: Boolean = true,
      authenticate: Boolean = true,
      parallelism: Int = 1
  ): Flow[HttpRequest, T, Future[HostConnectionPool]] =
    Flow[HttpRequest]
      .map((_, ()))
      .viaMat(cachedHostConnectionPoolWithContext[T, Unit](host, port, https, authenticate, parallelism).asFlow)(
        Keep.right
      )
      .map(_._1.get)

  /**
   * Creates a cached host connection pool that sends requests and emits the [[Unmarshal]]led response.
   * If `authenticate = true` adds an Authorization header to each request.
   * Retries the request if the [[FromResponseUnmarshaller]] throws a [[akka.stream.alpakka.google.util.Retry]].
   */
  def cachedHostConnectionPoolWithContext[T: FromResponseUnmarshaller, Ctx](
      host: String,
      port: Int = -1,
      https: Boolean = true,
      authenticate: Boolean = true,
      parallelism: Int = 1
  ): FlowWithContext[HttpRequest, Ctx, Try[T], Ctx, Future[HostConnectionPool]] = FlowWithContext.fromTuples {
    Flow.fromMaterializer { (mat, attr) =>
      implicit val settings = GoogleAttributes.resolveSettings(mat, attr)
      val p = if (port == -1) if (https) 443 else 80 else port

      val uriFlow = FlowWithContext[HttpRequest, Ctx].map(addStandardQuery)

      val authFlow =
        if (authenticate)
          FlowWithContext[HttpRequest, Ctx].mapAsync(1)(addAuth)
        else
          FlowWithContext[HttpRequest, Ctx]

      val requestFlow = settings.requestSettings.forwardProxy match {
        case None if !https =>
          http.cachedHostConnectionPool[Ctx](host, p)
        case Some(proxy) if !https =>
          http.cachedHostConnectionPool[Ctx](host, p, proxy.poolSettings)
        case None if https =>
          http.cachedHostConnectionPoolHttps[Ctx](host, p)
        case Some(proxy) if https =>
          http.cachedHostConnectionPoolHttps[Ctx](host, p, proxy.connectionContext, proxy.poolSettings)
      }

      val unmarshalFlow = Flow[(Try[HttpResponse], Ctx)].mapAsyncUnordered(parallelism) {
        case (res, ctx) =>
          Future
            .fromTry(res)
            .flatMap(Unmarshal(_).to[T])(ExecutionContexts.parasitic)
            .transform(Success(_))(ExecutionContexts.parasitic)
            .zip(Future.successful(ctx))
      }

      val flow = uriFlow.via(authFlow).viaMat(requestFlow)(Keep.right).via(unmarshalFlow).asFlow

      val RetrySettings(maxRetries, minBackoff, maxBackoff, randomFactor) = settings.requestSettings.retrySettings
      RetryFlow.withBackoff(minBackoff, maxBackoff, randomFactor, maxRetries, flow) {
        case (in, (Failure(Retry(_)), _)) => Some(in)
        case _ => None
      } map {
        case (Failure(Retry(ex)), ctx) => (Failure(ex), ctx)
        case x => x
      }
    }
  }

  private def addStandardQuery(request: HttpRequest)(implicit settings: GoogleSettings): HttpRequest =
    request.withUri(
      request.uri.copy(
        rawQueryString = Some(
          request.uri.rawQueryString
            .fold(settings.requestSettings.queryString)(_.concat(settings.requestSettings.`&queryString`))
        )
      )
    )

  private def addAuth(request: HttpRequest)(implicit settings: GoogleSettings): Future[HttpRequest] = {
    implicit val requestSettings = settings.requestSettings
    settings.credentials
      .getToken()
      .map { token =>
        request.addHeader(Authorization(token))
      }(ExecutionContexts.parasitic)
  }
}
