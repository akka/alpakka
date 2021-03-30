/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.huawei.pushkit.impl

import akka.actor.ActorSystem
import akka.annotation.InternalApi
import akka.http.scaladsl.HttpExt
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers.{Authorization, OAuth2BearerToken}
import akka.http.scaladsl.unmarshalling.Unmarshal
import akka.stream.Materializer
import akka.stream.alpakka.huawei.pushkit.ForwardProxyHttpsContext.ForwardProxyHttpsContext
import akka.stream.alpakka.huawei.pushkit.ForwardProxyPoolSettings.ForwardProxyPoolSettings
import akka.stream.alpakka.huawei.pushkit.HmsSettings
import akka.stream.alpakka.huawei.pushkit.models.{ErrorResponse, PushKitResponse, Response}
import spray.json.enrichAny

import scala.collection.immutable
import scala.concurrent.{ExecutionContext, Future}

/**
 * INTERNAL API
 */
@InternalApi
private[pushkit] class PushKitSender {
  import PushKitJsonSupport._

  def send(conf: HmsSettings, token: String, http: HttpExt, hmsSend: PushKitSend, system: ActorSystem)(
      implicit materializer: Materializer
  ): Future[Response] = {
    val appId = conf.appId
    val forwardProxy = conf.forwardProxy
    val url = s"https://push-api.cloud.huawei.com/v1/$appId/messages:send"

    val response = forwardProxy match {
      case Some(fp) =>
        http.singleRequest(
          HttpRequest(
            HttpMethods.POST,
            url,
            immutable.Seq(Authorization(OAuth2BearerToken(token))),
            HttpEntity(ContentTypes.`application/json`, hmsSend.toJson.compactPrint)
          ),
          connectionContext = fp.httpsContext(system),
          settings = fp.poolSettings(system)
        )
      case None =>
        http.singleRequest(
          HttpRequest(
            HttpMethods.POST,
            url,
            immutable.Seq(Authorization(OAuth2BearerToken(token))),
            HttpEntity(ContentTypes.`application/json`, hmsSend.toJson.compactPrint)
          )
        )
    }
    parse(response)
  }

  private def parse(response: Future[HttpResponse])(implicit materializer: Materializer): Future[Response] = {
    implicit val executionContext: ExecutionContext = materializer.executionContext
    response.flatMap { rsp =>
      if (rsp.status.isSuccess) {
        Unmarshal(rsp.entity).to[PushKitResponse]
      } else {
        Unmarshal(rsp.entity).to[ErrorResponse]
      }
    }
  }
}
