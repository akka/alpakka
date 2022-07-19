/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.typesense.scaladsl

import akka.actor.ActorSystem
import akka.http.scaladsl.model.HttpMethods
import akka.stream.alpakka.typesense.impl.TypesenseHttp
import akka.stream.alpakka.typesense._
import akka.stream.alpakka.typesense.impl.CollectionResponses.IndexManyDocumentsResponse
import akka.stream.alpakka.typesense.impl.TypesenseHttp.{ParseResponse, PrepareRequestEntity}
import akka.stream.scaladsl.{Flow, Keep, Sink}
import akka.{Done, NotUsed}

import scala.concurrent.Future

/**
 * Scala DSL for Typesense
 */
object Typesense {
  import akka.stream.alpakka.typesense.impl.TypesenseJsonProtocol._
  import spray.json._

  /**
   * Creates a collection.
   */
  def createCollectionRequest(settings: TypesenseSettings,
                              schema: CollectionSchema)(implicit system: ActorSystem): Future[CollectionResponse] = {
    TypesenseHttp
      .executeRequest("collections",
                      HttpMethods.POST,
                      PrepareRequestEntity.json(schema.toJson),
                      settings,
                      ParseResponse.json[CollectionResponse])
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
      .executeRequest(s"collections/${retrieve.collectionName}",
                      HttpMethods.GET,
                      PrepareRequestEntity.empty,
                      settings,
                      ParseResponse.json[CollectionResponse])

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

  /**
   * Index a single document.
   */
  def indexDocumentRequest[T: JsonWriter](settings: TypesenseSettings, document: IndexDocument[T])(
      implicit system: ActorSystem
  ): Future[Done] =
    TypesenseHttp
      .executeRequest(
        s"collections/${document.collectionName}/documents",
        HttpMethods.POST,
        PrepareRequestEntity.json(document.content.toJson),
        settings,
        ParseResponse.withoutBody,
        Map("action" -> indexActionValue(document.action))
      )(system)

  /**
   * Creates a flow for indexing a single document.
   */
  def indexDocumentFlow[T: JsonWriter](
      settings: TypesenseSettings
  ): Flow[IndexDocument[T], Done, Future[NotUsed]] =
    Flow.fromMaterializer { (materializer, _) =>
      implicit val system: ActorSystem = materializer.system
      Flow[IndexDocument[T]]
        .mapAsync(parallelism = 1) { document =>
          indexDocumentRequest(settings, document)
        }
    }

  /**
   * Index many documents.
   */
  def indexManyDocumentsRequest[T: JsonWriter](settings: TypesenseSettings, index: IndexManyDocuments[T])(
      implicit system: ActorSystem
  ): Future[Seq[IndexDocumentResult]] =
    TypesenseHttp
      .executeRequest(
        s"collections/${index.collectionName}/documents/import",
        HttpMethods.POST,
        PrepareRequestEntity.jsonLine(index.documents.map(_.toJson)),
        settings,
        ParseResponse.jsonLine[IndexManyDocumentsResponse],
        Map("action" -> indexActionValue(index.action))
      )(system)
      .map(_.map(_.asResult))(system.dispatcher)

  /**
   * Creates a flow for indexing many documents.
   */
  def indexManyDocumentsFlow[T: JsonWriter](
      settings: TypesenseSettings
  ): Flow[IndexManyDocuments[T], Seq[IndexDocumentResult], Future[NotUsed]] =
    Flow.fromMaterializer { (materializer, _) =>
      implicit val system: ActorSystem = materializer.system
      Flow[IndexManyDocuments[T]]
        .mapAsync(parallelism = 1) { index =>
          indexManyDocumentsRequest(settings, index)
        }
    }

  /**
   * Creates a sink for indexing a single document.
   */
  def indexDocumentSink[T: JsonWriter](
      settings: TypesenseSettings
  ): Sink[IndexDocument[T], Future[Done]] =
    indexDocumentFlow[T](settings).toMat(Sink.ignore)(Keep.right)

  /**
   * Retrieve a document.
   */
  def retrieveDocumentRequest[T: JsonReader](settings: TypesenseSettings, retrieve: RetrieveDocument)(
      implicit system: ActorSystem
  ): Future[T] =
    TypesenseHttp
      .executeRequest(
        s"collections/${retrieve.collectionName}/documents/${retrieve.documentId}",
        HttpMethods.GET,
        PrepareRequestEntity.empty,
        settings,
        ParseResponse.json[T]
      )(system)

  /**
   * Creates a flow for retrieving a document.
   */
  def retrieveDocumentFlow[T: JsonReader](
      settings: TypesenseSettings
  ): Flow[RetrieveDocument, T, Future[NotUsed]] =
    Flow.fromMaterializer { (materializer, _) =>
      implicit val system: ActorSystem = materializer.system
      Flow[RetrieveDocument]
        .mapAsync(parallelism = 1) { document =>
          retrieveDocumentRequest[T](settings, document)
        }
    }

  /**
   * Delete a single document.
   */
  def deleteDocumentRequest(settings: TypesenseSettings, delete: DeleteDocument)(
      implicit system: ActorSystem
  ): Future[Done] =
    TypesenseHttp
      .executeRequest(
        s"collections/${delete.collectionName}/documents/${delete.documentId}",
        HttpMethods.DELETE,
        PrepareRequestEntity.empty,
        settings,
        ParseResponse.withoutBody
      )

  /**
   * Creates a flow for deleting a single document.
   */
  def deleteDocumentFlow(
      settings: TypesenseSettings
  ): Flow[DeleteDocument, Done, Future[NotUsed]] =
    Flow.fromMaterializer { (materializer, _) =>
      implicit val system: ActorSystem = materializer.system
      Flow[DeleteDocument]
        .mapAsync(parallelism = 1) { document =>
          deleteDocumentRequest(settings, document)
        }
    }

  /**
   * Creates a sink for deleting a single document.
   */
  def deleteDocumentSink(
      settings: TypesenseSettings
  ): Sink[DeleteDocument, Future[Done]] =
    deleteDocumentFlow(settings).toMat(Sink.ignore)(Keep.right)

  private def indexActionValue(action: IndexDocumentAction): String = action match {
    case IndexDocumentAction.Create => "create"
    case IndexDocumentAction.Upsert => "upsert"
    case IndexDocumentAction.Update => "update"
    case IndexDocumentAction.Emplace => "emplace"
  }

  /**
   * Delete many documents by query.
   */
  def deleteManyDocumentsByQueryRequest(settings: TypesenseSettings, delete: DeleteManyDocumentsByQuery)(
      implicit system: ActorSystem
  ): Future[DeleteManyDocumentsResult] =
    TypesenseHttp
      .executeRequest(
        s"collections/${delete.collectionName}/documents",
        HttpMethods.DELETE,
        PrepareRequestEntity.empty,
        settings,
        ParseResponse.json[DeleteManyDocumentsResult],
        Map("filter_by" -> delete.filterBy.asTextQuery) ++ delete.batchSize.map("batch_size" -> _.toString)
      )

  /**
   * Creates a flow for deleting many documents by query.
   */
  def deleteManyDocumentsByQueryFlow(
      settings: TypesenseSettings
  ): Flow[DeleteManyDocumentsByQuery, DeleteManyDocumentsResult, Future[NotUsed]] =
    Flow.fromMaterializer { (materializer, _) =>
      implicit val system: ActorSystem = materializer.system
      Flow[DeleteManyDocumentsByQuery]
        .mapAsync(parallelism = 1) { document =>
          deleteManyDocumentsByQueryRequest(settings, document)
        }
    }
}
