/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.bigquery.impl.sendrequest

import akka.actor.ActorSystem
import akka.annotation.InternalApi
import akka.http.scaladsl.HttpExt
import akka.http.scaladsl.model.{HttpRequest, HttpResponse}
import akka.http.scaladsl.unmarshalling.Unmarshal
import akka.stream.Materializer
import akka.stream.alpakka.googlecloud.bigquery.BigQueryConfig
import akka.stream.scaladsl.Flow
import akka.stream.alpakka.googlecloud.bigquery.scaladsl.ForwardProxyPoolSettings._
import akka.stream.alpakka.googlecloud.bigquery.scaladsl.ForwardProxyHttpsContext._


import scala.concurrent.{ExecutionContext, Future}

@InternalApi
private[bigquery] object SendRequestWithOauthHandling {

  def apply(bigQueryConfig: BigQueryConfig, http: HttpExt)(
      implicit mat: Materializer,
      system: ActorSystem
  ) =
    Flow[HttpRequest]
      .via(EnrichRequestWithOauth(bigQueryConfig.session))
      .mapAsync(1)(
        bigQueryConfig.forwardProxy match {
          case Some(fp) => http.singleRequest(_, connectionContext = fp.httpsContext(system) ,settings = fp.poolSettings(system))
          case None => http.singleRequest(_)
        }
      )
      .mapAsync(1)(handleRequestError(_))

  private def handleRequestError(response: HttpResponse)(implicit materializer: Materializer) = {
    implicit val ec: ExecutionContext = materializer.executionContext
    if (response.status.isFailure) {
      Unmarshal(response.entity)
        .to[String]
        .map(
          errorBody => throw new IllegalStateException(s"Unexpected error in response: ${response.status}, $errorBody")
        )
    } else {
      Future.successful(response)
    }
  }
}
