/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.typesense.impl

import akka.Done
import akka.actor.ActorSystem
import akka.annotation.InternalApi
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.StatusCodes.{
  BadGateway,
  GatewayTimeout,
  InternalServerError,
  ServiceUnavailable,
  TooManyRequests
}
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers.RawHeader
import akka.http.scaladsl.unmarshalling.Unmarshal
import akka.stream.alpakka.typesense.{
  FailureTypesenseResult,
  SuccessTypesenseResult,
  TypesenseResult,
  TypesenseSettings
}
import spray.json.{JsValue, JsonReader}

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Success, Try}

@InternalApi private[typesense] object TypesenseHttp {
  object PrepareRequestEntity {
    def json(body: JsValue): RequestEntity = HttpEntity(ContentTypes.`application/json`, body.prettyPrint)
    def jsonLine(body: Seq[JsValue]): RequestEntity =
      HttpEntity(ContentTypes.`text/plain(UTF-8)`, body.map(_.compactPrint).mkString("\n"))
    val empty: RequestEntity = HttpEntity.Empty
  }

  object ParseResponse {
    def jsonTypesenseResult[T: JsonReader](res: HttpResponse, system: ActorSystem): Future[TypesenseResult[T]] = {
      import spray.json._
      implicit val as: ActorSystem = system
      import system.dispatcher

      Unmarshal(res).to[String].flatMap { body: String =>
        prepareTypesenseResult(body, res.status, body => Try(body.parseJson.convertTo[T]))
      }
    }

    def json[T: JsonReader](res: HttpResponse, system: ActorSystem): Future[T] = {
      import system.dispatcher
      unwrapFutureWithTypesenseResult(jsonTypesenseResult(res, system))
    }

    def jsonLine[T: JsonReader](res: HttpResponse, system: ActorSystem): Future[Seq[T]] = {
      import spray.json._
      implicit val as: ActorSystem = system
      import system.dispatcher

      Unmarshal(res).to[String].flatMap { body: String =>
        if (res.status.isFailure())
          Future.failed(new RetryableTypesenseException(res.status, body))
        else
          Future.fromTry(
            Try(
              body.split("\n").toSeq.map(_.parseJson.convertTo[T])
            )
          )
      }
    }

    def withoutBodyTypesenseResult(res: HttpResponse, system: ActorSystem): Future[TypesenseResult[Done]] = {
      implicit val as: ActorSystem = system
      import system.dispatcher
      Unmarshal(res).to[String].flatMap { body: String =>
        prepareTypesenseResult(body, res.status, _ => Success(Done))
      }
    }

    private def prepareTypesenseResult[T](body: String, status: StatusCode, convert: String => Try[T])(
        implicit ec: ExecutionContext
    ): Future[TypesenseResult[T]] =
      status match {
        case TooManyRequests | InternalServerError | BadGateway | ServiceUnavailable | GatewayTimeout =>
          Future.failed(new RetryableTypesenseException(status, body))
        case status if status.isFailure() => Future.successful(new FailureTypesenseResult(status, body))
        case _ => Future.fromTry(convert(body)).map(new SuccessTypesenseResult(_))
      }

    private def unwrapFutureWithTypesenseResult[T](future: Future[TypesenseResult[T]])(
        implicit ec: ExecutionContext
    ): Future[T] =
      future.flatMap {
        case result: SuccessTypesenseResult[T] => Future.successful(result.value)
        case result: FailureTypesenseResult[T] =>
          Future.failed(new RetryableTypesenseException(result.statusCode, result.reason))
      }
  }

  final class RetryableTypesenseException @InternalApi private[typesense] (val statusCode: StatusCode,
                                                                           val reason: String)
      extends Exception(s"Retryable Typesense exception: [status code $statusCode]: $reason") {

    override def equals(other: Any): Boolean = other match {
      case that: RetryableTypesenseException =>
        statusCode == that.statusCode &&
        reason == that.reason
      case _ => false
    }

    override def hashCode(): Int = java.util.Objects.hash(statusCode, reason)
  }

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
