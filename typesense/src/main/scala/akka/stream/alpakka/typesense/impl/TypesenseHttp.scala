/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.typesense.impl

import akka.Done
import akka.actor.ActorSystem
import akka.annotation.InternalApi
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers.RawHeader
import akka.http.scaladsl.unmarshalling.Unmarshal
import akka.stream.alpakka.typesense.TypesenseSettings
import spray.json.{JsValue, JsonReader}

import scala.concurrent.Future
import scala.util.Try

@InternalApi private[typesense] object TypesenseHttp {
  object PrepareRequestEntity {
    def json(body: JsValue): RequestEntity = HttpEntity(ContentTypes.`application/json`, body.prettyPrint)
    def jsonLine(body: Seq[JsValue]): RequestEntity =
      HttpEntity(ContentTypes.`text/plain(UTF-8)`, body.map(_.compactPrint).mkString("\n"))
    val empty: RequestEntity = HttpEntity.Empty
  }

  object ParseResponse {
    def json[T: JsonReader](res: HttpResponse, system: ActorSystem): Future[T] = {
      import spray.json._
      implicit val as: ActorSystem = system
      import system.dispatcher

      Unmarshal(res).to[String].flatMap { body: String =>
        if (res.status.isFailure())
          Future.failed(new TypesenseException(res.status, body))
        else
          Future.fromTry(Try(body.parseJson.convertTo[T]))
      }
    }

    def jsonLine[T: JsonReader](res: HttpResponse, system: ActorSystem): Future[Seq[T]] = {
      import spray.json._
      implicit val as: ActorSystem = system
      import system.dispatcher

      Unmarshal(res).to[String].flatMap { body: String =>
        if (res.status.isFailure())
          Future.failed(new TypesenseException(res.status, body))
        else
          Future.fromTry(
            Try(
              body.split("\n").toSeq.map(_.parseJson.convertTo[T])
            )
          )
      }
    }

    def withoutBody(res: HttpResponse, system: ActorSystem): Future[Done] = {
      implicit val as: ActorSystem = system
      import system.dispatcher
      Unmarshal(res).to[String].flatMap { body: String =>
        if (res.status.isFailure())
          Future.failed(new TypesenseException(res.status, body))
        else
          Future.successful(Done)
      }
    }
  }

  class TypesenseException(val statusCode: StatusCode, val reason: String)
      extends Exception(s"[Status code $statusCode]: $reason")

  //TODO: error handling tests - ex. authentication
  def executeRequest[Response](
      endpoint: String,
      method: HttpMethod,
      requestEntity: RequestEntity,
      settings: TypesenseSettings,
      parseResponseBody: (HttpResponse, ActorSystem) => Future[Response],
      requestParameters: Map[String, String] = Map.empty
  )(implicit system: ActorSystem): Future[Response] = {
    import system.dispatcher

    val uri = {
      val baseUri = Uri(settings.host + "/" + endpoint)
      if (requestParameters.isEmpty) baseUri
      else baseUri.withQuery(Uri.Query(requestParameters))
    }

    val request = HttpRequest(
      method = method,
      uri = uri,
      headers = List(RawHeader("X-TYPESENSE-API-KEY", settings.apiKey)),
      entity = requestEntity
    )

    Http()
      .singleRequest(request)
      .flatMap(res => parseResponseBody(res, system))
  }
}
