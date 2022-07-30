/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.typesense.scaladsl

import akka.actor.ActorSystem
import akka.http.scaladsl.model.HttpMethods
import akka.stream.alpakka.typesense._
import akka.stream.alpakka.typesense.impl.CollectionResponses.IndexManyDocumentsResponse
import akka.stream.alpakka.typesense.impl.TypesenseHttp
import akka.stream.alpakka.typesense.impl.TypesenseHttp.{
  ParseResponse,
  PrepareRequestEntity,
  RetryableTypesenseException
}
import akka.stream.alpakka.typesense.impl.TypesenseJsonProtocol._
import akka.stream.scaladsl.{Flow, RetryFlow}
import akka.{Done, NotUsed}
import spray.json._

import scala.concurrent.Future
import scala.util.{Failure, Success, Try}

/**
 * Scala DSL for Typesense
 */
object Typesense {

  /**
   * Creates a flow for creating collections.
   */
  def createCollectionFlow(
      settings: TypesenseSettings
  ): Flow[CollectionSchema, TypesenseResult[CollectionResponse], Future[NotUsed]] =
    flowFlowRequest(settings) { (schema, system) =>
      TypesenseHttp
        .executeRequest("collections",
                        HttpMethods.POST,
                        PrepareRequestEntity.json(schema.toJson),
                        settings,
                        ParseResponse.jsonTypesenseResult[CollectionResponse])(system)
    }

  /**
   * Creates a flow for retrieving collections.
   */
  def retrieveCollectionFlow(
      settings: TypesenseSettings
  ): Flow[RetrieveCollection, TypesenseResult[CollectionResponse], Future[NotUsed]] =
    flowFlowRequest(settings) { (retrieve, system) =>
      TypesenseHttp
        .executeRequest(s"collections/${retrieve.collectionName}",
                        HttpMethods.GET,
                        PrepareRequestEntity.empty,
                        settings,
                        ParseResponse.jsonTypesenseResult[CollectionResponse])(system)
    }

  /**
   * Creates a flow for indexing a single document.
   */
  def indexDocumentFlow[T: JsonWriter](
      settings: TypesenseSettings
  ): Flow[IndexDocument[T], TypesenseResult[Done], Future[NotUsed]] =
    flowFlowRequest(settings) { (document, system) =>
      TypesenseHttp
        .executeRequest(
          s"collections/${document.collectionName}/documents",
          HttpMethods.POST,
          PrepareRequestEntity.json(document.content.toJson),
          settings,
          ParseResponse.withoutBodyTypesenseResult,
          Map("action" -> indexActionValue(document.action))
        )(system)
    }

  /**
   * Creates a flow for indexing many documents.
   */
  def indexManyDocumentsFlow[T: JsonWriter](
      settings: TypesenseSettings
  ): Flow[IndexManyDocuments[T], Seq[IndexDocumentResult], Future[NotUsed]] =
    flowFlowRequest(settings) { (index, system) =>
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
    }

  /**
   * Creates a flow for retrieving a document.
   */
  def retrieveDocumentFlow[T: JsonReader](
      settings: TypesenseSettings
  ): Flow[RetrieveDocument, TypesenseResult[T], Future[NotUsed]] =
    flowFlowRequest(settings)(
      (retrieve, system) =>
        TypesenseHttp
          .executeRequest(
            s"collections/${retrieve.collectionName}/documents/${retrieve.documentId}",
            HttpMethods.GET,
            PrepareRequestEntity.empty,
            settings,
            ParseResponse.jsonTypesenseResult[T]
          )(system)
    )

  /**
   * Creates a flow for deleting a single document.
   */
  def deleteDocumentFlow(
      settings: TypesenseSettings
  ): Flow[DeleteDocument, TypesenseResult[Done], Future[NotUsed]] =
    flowFlowRequest(settings)(
      (delete, system) =>
        TypesenseHttp
          .executeRequest(
            s"collections/${delete.collectionName}/documents/${delete.documentId}",
            HttpMethods.DELETE,
            PrepareRequestEntity.empty,
            settings,
            ParseResponse.withoutBodyTypesenseResult
          )(system)
    )

  /**
   * Creates a flow for deleting many documents by query.
   */
  def deleteManyDocumentsByQueryFlow(
      settings: TypesenseSettings
  ): Flow[DeleteManyDocumentsByQuery, DeleteManyDocumentsResult, Future[NotUsed]] =
    flowFlowRequest(settings) { (delete, system) =>
      TypesenseHttp
        .executeRequest(
          s"collections/${delete.collectionName}/documents",
          HttpMethods.DELETE,
          PrepareRequestEntity.empty,
          settings,
          ParseResponse.json[DeleteManyDocumentsResult],
          Map("filter_by" -> delete.filterBy.asTextQuery) ++ delete.batchSize.map("batch_size" -> _.toString)
        )(system)
    }

  private def flowFlowRequest[Request, Response](
      settings: TypesenseSettings
  )(future: (Request, ActorSystem) => Future[Response]): Flow[Request, Response, Future[NotUsed]] = {

    val basicFlow: Flow[Request, Try[Response], Future[NotUsed]] = Flow.fromMaterializer { (materializer, _) =>
      implicit val system: ActorSystem = materializer.system
      import system.dispatcher
      Flow[Request]
        .mapAsync(parallelism = 1) { req =>
          future(req, system).map(Success.apply).recover(e => Failure(e))
        }
    }
    val retrySettings = settings.retrySettings
    val retryFlow = RetryFlow.withBackoff(
      minBackoff = retrySettings.minBackoff,
      maxBackoff = retrySettings.maxBackoff,
      randomFactor = retrySettings.randomFactor,
      maxRetries = retrySettings.maxRetries,
      flow = basicFlow
    ) {
      case (req, Failure(_: RetryableTypesenseException)) => Some(req) //retry
      case (_, Success(_)) => None //success
      case (req, Failure(_)) => None //failure, but non retryable, in next flow exception will be thrown
    }

    retryFlow.map(_.get)
  }

  private def indexActionValue(action: IndexDocumentAction): String = action match {
    case IndexDocumentAction.Create => "create"
    case IndexDocumentAction.Upsert => "upsert"
    case IndexDocumentAction.Update => "update"
    case IndexDocumentAction.Emplace => "emplace"
  }
}
