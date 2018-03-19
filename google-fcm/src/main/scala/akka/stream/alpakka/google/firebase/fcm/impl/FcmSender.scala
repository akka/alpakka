/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.google.firebase.fcm.impl

import akka.http.scaladsl.HttpExt
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers.{Authorization, OAuth2BearerToken}
import akka.http.scaladsl.unmarshalling.Unmarshal
import akka.stream.Materializer
import akka.stream.alpakka.google.firebase.fcm.FcmFlowModels.{
  FcmErrorResponse,
  FcmResponse,
  FcmSend,
  FcmSuccessResponse
}
import spray.json._

import scala.collection.immutable
import scala.concurrent.{ExecutionContext, Future}

private[fcm] class FcmSender extends FcmJsonSupport {

  def send(projectId: String, token: String, http: HttpExt, fcmSend: FcmSend)(
      implicit materializer: Materializer
  ): Future[FcmResponse] = {
    val url = s"https://fcm.googleapis.com/v1/projects/$projectId/messages:send"

    val response = http.singleRequest(
      HttpRequest(
        HttpMethods.POST,
        url,
        immutable.Seq(Authorization(OAuth2BearerToken(token))),
        HttpEntity(ContentTypes.`application/json`, fcmSend.toJson.compactPrint)
      )
    )
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
