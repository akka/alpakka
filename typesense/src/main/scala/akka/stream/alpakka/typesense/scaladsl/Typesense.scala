/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.typesense.scaladsl

import akka.actor.ActorSystem
import akka.http.scaladsl.model.HttpMethods
import akka.stream.alpakka.typesense.impl.TypesenseHttp
import akka.stream.alpakka.typesense.{CollectionResponse, CollectionSchema, RetrieveCollection, TypesenseSettings}
import akka.stream.scaladsl.{Flow, Keep, Sink}
import akka.{Done, NotUsed}

import scala.concurrent.Future

/**
 * Scala DSL for Typesense
 */
object Typesense {
  import akka.stream.alpakka.typesense.impl.TypesenseJsonProtocol._

  /**
   * Creates a collection.
   */
  def createCollectionRequest(settings: TypesenseSettings,
                              schema: CollectionSchema)(implicit system: ActorSystem): Future[CollectionResponse] = {
    TypesenseHttp
      .executeRequestWithBody[CollectionSchema, CollectionResponse]("collections", HttpMethods.POST, schema, settings)
  }

  /**
   * Creates a flow for creating collections.
   */
  def createCollectionFlow(settings: TypesenseSettings): Flow[CollectionSchema, CollectionResponse, Future[NotUsed]] =
    Flow.fromMaterializer { (materializer, _) =>
      implicit val system: ActorSystem = materializer.system
      Flow[CollectionSchema]
        .mapAsync(parallelism = 1) { schema =>
          createCollectionRequest(settings, schema)
        }
    }

  /**
   * Creates a sink for creating collections.
   */
  def createCollectionSink(settings: TypesenseSettings): Sink[CollectionSchema, Future[Done]] =
    createCollectionFlow(settings).toMat(Sink.ignore)(Keep.right)

  /**
   * Retrieve a collection.
   */
  def retrieveCollectionRequest(settings: TypesenseSettings, retrieve: RetrieveCollection)(
      implicit system: ActorSystem
  ): Future[CollectionResponse] =
    TypesenseHttp
      .executeGetRequestWithoutBody[CollectionResponse](s"collections/${retrieve.collectionName}", settings)

  /**
   * Creates a flow for retrieving collections.
   */
  def retrieveCollectionFlow(
      settings: TypesenseSettings
  ): Flow[RetrieveCollection, CollectionResponse, Future[NotUsed]] =
    Flow.fromMaterializer { (materializer, _) =>
      implicit val system: ActorSystem = materializer.system
      Flow[RetrieveCollection]
        .mapAsync(parallelism = 1) { retrieve =>
          retrieveCollectionRequest(settings, retrieve)
        }
    }
}
