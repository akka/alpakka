package akka.stream.alpakka.typesense.scaladsl

import akka.NotUsed
import akka.http.scaladsl.model.HttpMethods
import akka.stream.Materializer
import akka.stream.alpakka.typesense.impl.TypesenseHttp
import akka.stream.alpakka.typesense.{CollectionResponse, CollectionSchema, TypesenseSettings}
import akka.stream.scaladsl.Flow

import scala.concurrent.Future

/**
 * Scala DSL for Google Pub/Sub
 */
object Typesense {
  def createCollectionFlow(settings: TypesenseSettings): Flow[CollectionSchema, CollectionResponse, Future[NotUsed]] =
    Flow.fromMaterializer { (materializer, _) =>
      import akka.stream.alpakka.typesense.TypesenseJsonProtocol._
      implicit val implMaterializer: Materializer = materializer
      Flow[CollectionSchema]
        .mapAsync(parallelism = 1) { schema =>
          TypesenseHttp
            .executeRequest[CollectionSchema, CollectionResponse]("collections", HttpMethods.POST, schema, settings)
        }
    }
}
