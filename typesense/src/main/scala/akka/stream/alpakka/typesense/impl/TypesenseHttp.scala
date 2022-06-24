package akka.stream.alpakka.typesense.impl

import akka.actor.ClassicActorSystemProvider
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.headers.RawHeader
import akka.http.scaladsl.model.{ContentTypes, HttpEntity, HttpMethods, HttpRequest}
import akka.http.scaladsl.unmarshalling.Unmarshal
import akka.stream.alpakka.typesense.{CollectionResponse, CollectionSchema, TypesenseSettings}

import scala.concurrent.{ExecutionContextExecutor, Future}

object TypesenseHttp {
  def createCollectionRequest(
      schema: CollectionSchema,
      settings: TypesenseSettings
  )(implicit system: ClassicActorSystemProvider, ec: ExecutionContextExecutor): Future[CollectionResponse] = {
    import akka.stream.alpakka.typesense.TypesenseJsonProtocol._
    import spray.json._

    val request = HttpRequest(
      method = HttpMethods.POST,
      uri = settings.host + "/collections",
      headers = List(RawHeader("X-TYPESENSE-API-KEY", settings.apiKey)),
      entity = HttpEntity(
        ContentTypes.`application/json`,
        schema.toJson.prettyPrint
      )
    )

    Http()
      .singleRequest(request)
      .flatMap { res =>
        Unmarshal(res).to[String].map { data: String =>
          data.parseJson.convertTo[CollectionResponse]
        }
      }
  }
}
