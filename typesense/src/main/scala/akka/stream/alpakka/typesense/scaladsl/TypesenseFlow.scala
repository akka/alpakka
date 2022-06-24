package akka.stream.alpakka.typesense.scaladsl

import akka.NotUsed
import akka.stream.alpakka.typesense.impl.TypesenseHttp
import akka.stream.alpakka.typesense.{CollectionResponse, CollectionSchema, TypesenseSettings}
import akka.stream.scaladsl.Flow

import scala.concurrent.Future

object TypesenseFlow {
  def createCollection(settings: TypesenseSettings): Flow[CollectionSchema, CollectionResponse, Future[NotUsed]] = {
    Flow.fromMaterializer { (materializer, _) =>
      Flow[CollectionSchema]
        .mapAsync(parallelism = 1) { schema =>
          TypesenseHttp.createCollectionRequest(schema, settings)(materializer.system, materializer.executionContext)
        }
    }
  }
}
