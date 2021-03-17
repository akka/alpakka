/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.google

import akka.annotation.InternalApi
import akka.http.scaladsl.model.HttpResponse
import akka.http.scaladsl.model.StatusCodes._
import akka.http.scaladsl.model.Uri.Query
import akka.http.scaladsl.unmarshalling.{FromEntityUnmarshaller, FromResponseUnmarshaller, Unmarshal, Unmarshaller}
import akka.http.scaladsl.util.FastFuture.EnhancedFuture
import akka.stream.alpakka.google.util.Retry

@InternalApi
private[alpakka] object implicits {

  implicit final class QueryPrependOption(val query: Query) extends AnyVal {
    def ?+:(kv: (String, Option[Any])): Query = kv._2.fold(query)(v => Query.Cons(kv._1, v.toString, query))
  }

  implicit final class FromResponseUnmarshallerWithDefaultRetry[T](val um: FromResponseUnmarshaller[T]) extends AnyVal {

    /**
     * Automatically identifies retryable exceptions with reasonable defaults. Should be sufficient for most APIs.
     */
    def withDefaultRetry: FromResponseUnmarshaller[T] =
      Unmarshaller.withMaterializer { implicit ec => implicit mat => response =>
        um(response).recoverWith {
          case ex =>
            Unmarshaller.strict((_: HttpResponse) => ex).withDefaultRetry.apply(response).fast.map(throw _)
        }
      }
  }

  implicit final class ToThrowableUnmarshallerWithDefaultRetry(val um: FromResponseUnmarshaller[Throwable])
      extends AnyVal {

    /**
     * Automatically identifies retryable exceptions with reasonable defaults. Should be sufficient for most APIs.
     */
    def withDefaultRetry: FromResponseUnmarshaller[Throwable] =
      Unmarshaller.withMaterializer { implicit ec => implicit mat => response =>
        um(response).map {
          case ex =>
            response.status match {
              case TooManyRequests | InternalServerError | BadGateway | ServiceUnavailable | GatewayTimeout => Retry(ex)
              case _ => ex
            }
        }
      }
  }

  /**
   * Merges a success and failure unmarshaller into a single unmarshaller
   */
  implicit def responseUnmarshallerWithExceptions[T](
      implicit um: FromEntityUnmarshaller[T],
      exUm: FromResponseUnmarshaller[Throwable]
  ): FromResponseUnmarshaller[T] =
    Unmarshaller.withMaterializer { implicit ec => implicit mat => response =>
      if (response.status.isSuccess())
        Unmarshal(response.entity).to[T]
      else
        Unmarshal(response).to[Throwable].map(throw _)
    }
}
