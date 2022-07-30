/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.typesense.scaladsl

import akka.actor.ActorSystem
import akka.http.scaladsl.model.HttpMethods
import akka.stream.alpakka.typesense._
import akka.stream.alpakka.typesense.impl.CollectionResponses.IndexManyDocumentsResponse
import akka.stream.alpakka.typesense.impl.TypesenseHttp
import akka.stream.alpakka.typesense.impl.TypesenseHttp.{ParseResponse, PrepareRequestEntity}
import akka.stream.alpakka.typesense.impl.TypesenseJsonProtocol._
import akka.stream.scaladsl.Flow
import akka.{Done, NotUsed}
import spray.json._

import scala.concurrent.Future

/**
 * Scala DSL for Typesense
 */
object Typesense {

  /**
   * Creates a flow for creating collections.
   */
  def createCollectionFlow(settings: TypesenseSettings): Flow[CollectionSchema, CollectionResponse, Future[NotUsed]] =
    Flow.fromMaterializer { (materializer, _) =>
      implicit val system: ActorSystem = materializer.system
      Flow[CollectionSchema]
        .mapAsync(parallelism = 1) { schema =>
          TypesenseHttp
            .executeRequest("collections",
                            HttpMethods.POST,
                            PrepareRequestEntity.json(schema.toJson),
                            settings,
                            ParseResponse.json[CollectionResponse])
        }
    }

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
          TypesenseHttp
            .executeRequest(s"collections/${retrieve.collectionName}",
                            HttpMethods.GET,
                            PrepareRequestEntity.empty,
                            settings,
                            ParseResponse.json[CollectionResponse])
        }
    }

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
          TypesenseHttp
            .executeRequest(
              s"collections/${document.collectionName}/documents",
              HttpMethods.POST,
              PrepareRequestEntity.json(document.content.toJson),
              settings,
              ParseResponse.withoutBody,
              Map("action" -> indexActionValue(document.action))
            )
        }
    }

  /**
   * Creates a flow for indexing many documents.
   */
  def indexManyDocumentsFlow[T: JsonWriter](
      settings: TypesenseSettings
  ): Flow[IndexManyDocuments[T], Seq[IndexDocumentResult], Future[NotUsed]] =
    Flow.fromMaterializer { (materializer, _) =>
      implicit val system: ActorSystem = materializer.system
      import system.dispatcher
      Flow[IndexManyDocuments[T]]
        .mapAsync(parallelism = 1) { index =>
          TypesenseHttp
            .executeRequest(
              s"collections/${index.collectionName}/documents/import",
              HttpMethods.POST,
              PrepareRequestEntity.jsonLine(index.documents.map(_.toJson)),
              settings,
              ParseResponse.jsonLine[IndexManyDocumentsResponse],
              Map("action" -> indexActionValue(index.action))
            )
            .map(_.map(_.asResult))
        }
    }

  /**
   * Creates a flow for retrieving a document.
   */
  def retrieveDocumentFlow[T: JsonReader](
      settings: TypesenseSettings
  ): Flow[RetrieveDocument, T, Future[NotUsed]] =
    Flow.fromMaterializer { (materializer, _) =>
      implicit val system: ActorSystem = materializer.system
      Flow[RetrieveDocument]
        .mapAsync(parallelism = 1) { retrieve =>
          TypesenseHttp
            .executeRequest(
              s"collections/${retrieve.collectionName}/documents/${retrieve.documentId}",
              HttpMethods.GET,
              PrepareRequestEntity.empty,
              settings,
              ParseResponse.json[T]
            )
        }
    }

  /**
   * Creates a flow for deleting a single document.
   */
  def deleteDocumentFlow(
      settings: TypesenseSettings
  ): Flow[DeleteDocument, Done, Future[NotUsed]] =
    Flow.fromMaterializer { (materializer, _) =>
      implicit val system: ActorSystem = materializer.system
      Flow[DeleteDocument]
        .mapAsync(parallelism = 1) { delete =>
          TypesenseHttp
            .executeRequest(
              s"collections/${delete.collectionName}/documents/${delete.documentId}",
              HttpMethods.DELETE,
              PrepareRequestEntity.empty,
              settings,
              ParseResponse.withoutBody
            )
        }
    }

  /**
   * Creates a flow for deleting many documents by query.
   */
  def deleteManyDocumentsByQueryFlow(
      settings: TypesenseSettings
  ): Flow[DeleteManyDocumentsByQuery, DeleteManyDocumentsResult, Future[NotUsed]] =
    Flow.fromMaterializer { (materializer, _) =>
      implicit val system: ActorSystem = materializer.system
      Flow[DeleteManyDocumentsByQuery]
        .mapAsync(parallelism = 1) { delete =>
          TypesenseHttp
            .executeRequest(
              s"collections/${delete.collectionName}/documents",
              HttpMethods.DELETE,
              PrepareRequestEntity.empty,
              settings,
              ParseResponse.json[DeleteManyDocumentsResult],
              Map("filter_by" -> delete.filterBy.asTextQuery) ++ delete.batchSize.map("batch_size" -> _.toString)
            )
        }
    }

  private def indexActionValue(action: IndexDocumentAction): String = action match {
    case IndexDocumentAction.Create => "create"
    case IndexDocumentAction.Upsert => "upsert"
    case IndexDocumentAction.Update => "update"
    case IndexDocumentAction.Emplace => "emplace"
  }
}
