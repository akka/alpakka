package akka.stream.alpakka.typesense.scaladsl

import akka.{Done, NotUsed}
import akka.http.scaladsl.model.HttpMethods
import akka.stream.Materializer
import akka.stream.alpakka.typesense.impl.TypesenseHttp
import akka.stream.alpakka.typesense.{CollectionResponse, CollectionSchema, TypesenseSettings}
import akka.stream.scaladsl.{Flow, Keep, Sink}

import scala.concurrent.Future

/**
 * Scala DSL for Google Pub/Sub
 */
object Typesense {

  /**
   * Creates a flow for creating collections.
   */
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

  /**
   * Creates a sink for creating collections.
   */
  def createCollection(settings: TypesenseSettings): Sink[CollectionSchema, Future[Done]] =
    createCollectionFlow(settings).toMat(Sink.ignore)(Keep.right)
}
