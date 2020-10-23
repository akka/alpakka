/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.google.firebase.fcm.impl

import akka.actor.ActorSystem
import akka.http.scaladsl.HttpExt
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers.{Authorization, OAuth2BearerToken}
import akka.http.scaladsl.unmarshalling.Unmarshal
import akka.stream.Materializer
import akka.stream.alpakka.google.firebase.fcm.{FcmErrorResponse, FcmResponse, FcmSettings, FcmSuccessResponse}
import akka.annotation.InternalApi
import akka.stream.alpakka.google.firebase.fcm.ForwardProxyHttpsContext.ForwardProxyHttpsContext
import akka.stream.alpakka.google.firebase.fcm.ForwardProxyPoolSettings.ForwardProxyPoolSettings
import spray.json._

import scala.collection.immutable
import scala.concurrent.{ExecutionContext, Future}

/**
 * INTERNAL API
 */
@InternalApi
private[fcm] class FcmSender {
  import FcmJsonSupport._

  def send(conf: FcmSettings, token: String, http: HttpExt, fcmSend: FcmSend, system: ActorSystem)(
      implicit materializer: Materializer
  ): Future[FcmResponse] = {
    val projectId = conf.projectId
    val forwardProxy = conf.forwardProxy
    val url = s"https://fcm.googleapis.com/v1/projects/$projectId/messages:send"

    val response = forwardProxy match {
      case Some(fp) =>
        http.singleRequest(
          HttpRequest(
            HttpMethods.POST,
            url,
            immutable.Seq(Authorization(OAuth2BearerToken(token))),
            HttpEntity(ContentTypes.`application/json`, fcmSend.toJson.compactPrint)
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
            HttpEntity(ContentTypes.`application/json`, fcmSend.toJson.compactPrint)
          )
        )
    }
    parse(response)
  }

  private def parse(response: Future[HttpResponse])(implicit materializer: Materializer): Future[FcmResponse] = {
    implicit val executionContext: ExecutionContext = materializer.executionContext
    response.flatMap { rsp =>
      if (rsp.status.isSuccess) {
        Unmarshal(rsp.entity).to[FcmSuccessResponse]
      } else {
        Unmarshal(rsp.entity).to[FcmErrorResponse]
      }
    }
  }
}
