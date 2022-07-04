/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.typesense.impl

import akka.actor.ActorSystem
import akka.annotation.InternalApi
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers.RawHeader
import akka.http.scaladsl.unmarshalling.Unmarshal
import akka.stream.alpakka.typesense.TypesenseSettings
import spray.json.{JsonReader, JsonWriter}

import scala.concurrent.Future
import scala.util.Try

@InternalApi private[typesense] object TypesenseHttp {
  class TypesenseException(val statusCode: StatusCode, val reason: String)
      extends Exception(s"[Status code $statusCode]: $reason")

  def executeGetRequestWithoutBody[Response: JsonReader](endpoint: String, settings: TypesenseSettings)(
      implicit system: ActorSystem
  ): Future[Response] =
    executeHttpRequest(
      HttpRequest(
        method = HttpMethods.GET,
        uri = settings.host + "/" + endpoint,
        headers = List(RawHeader("X-TYPESENSE-API-KEY", settings.apiKey))
      )
    )

  //TODO: error handling tests - ex. authentication
  def executeRequestWithBody[Request: JsonWriter, Response: JsonReader](
      endpoint: String,
      method: HttpMethod,
      requestData: Request,
      settings: TypesenseSettings
  )(implicit system: ActorSystem): Future[Response] = {
    import spray.json._

    val requestJson = requestData.toJson.prettyPrint

    val request = HttpRequest(
      method = method,
      uri = settings.host + "/" + endpoint,
      headers = List(RawHeader("X-TYPESENSE-API-KEY", settings.apiKey)),
      entity = HttpEntity(ContentTypes.`application/json`, requestJson)
    )

    executeHttpRequest(request)
  }

  private def executeHttpRequest[Response: JsonReader](
      request: HttpRequest
  )(implicit system: ActorSystem): Future[Response] = {
    import spray.json._
    import system.dispatcher

    Http()
      .singleRequest(request)
      .flatMap { res =>
        Unmarshal(res).to[String].flatMap { body: String =>
          if (res.status.isFailure())
            Future.failed(new TypesenseException(res.status, body))
          else
            Future.fromTry(Try(body.parseJson.convertTo[Response]))
        }
      }
  }

}
