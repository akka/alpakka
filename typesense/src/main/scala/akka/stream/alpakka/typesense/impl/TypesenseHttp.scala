package akka.stream.alpakka.typesense.impl

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers.RawHeader
import akka.http.scaladsl.unmarshalling.Unmarshal
import akka.stream.Materializer
import akka.stream.alpakka.typesense.TypesenseSettings
import spray.json.{JsonReader, JsonWriter}

import scala.concurrent.{ExecutionContextExecutor, Future}
import scala.util.Try

object TypesenseHttp {
  class TypesenseException(val statusCode: StatusCode, val reason: String)
      extends Exception(s"[Status code $statusCode]: $reason")

  //TODO: error handling tests - ex. authentication
  def executeRequest[Request: JsonWriter, Response: JsonReader](
      endpoint: String,
      method: HttpMethod,
      requestData: Request,
      settings: TypesenseSettings
  )(implicit materializer: Materializer): Future[Response] = {
    import spray.json._
    implicit val system: ActorSystem = materializer.system
    implicit val ec: ExecutionContextExecutor = materializer.executionContext

    val requestJson = requestData.toJson.prettyPrint

    val request = HttpRequest(
      method = method,
      uri = settings.host + "/" + endpoint,
      headers = List(RawHeader("X-TYPESENSE-API-KEY", settings.apiKey)),
      entity = HttpEntity(ContentTypes.`application/json`, requestJson)
    )

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
