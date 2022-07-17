/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.typesense.javadsl

import akka.actor.ActorSystem
import akka.stream.alpakka.typesense._
import akka.stream.javadsl.{Flow, Sink}
import akka.{Done, NotUsed}
import spray.json.{JsonReader, JsonWriter}

import java.util.concurrent.CompletionStage
import scala.compat.java8.FutureConverters.FutureOps
import scala.jdk.CollectionConverters.SeqHasAsJava

/**
 * Java DSL for Typesense
 */
object Typesense {
  private val ScalaTypesense = akka.stream.alpakka.typesense.scaladsl.Typesense

  /**
   * Creates a collection.
   */
  def createCollectionRequest(
      settings: TypesenseSettings,
      schema: CollectionSchema,
      system: ActorSystem
  ): CompletionStage[CollectionResponse] =
    ScalaTypesense
      .createCollectionRequest(settings, schema)(system)
      .toJava

  /**
   * Creates a flow for creating collections.
   */
  def createCollectionFlow(
      settings: TypesenseSettings
  ): Flow[CollectionSchema, CollectionResponse, CompletionStage[NotUsed]] =
    ScalaTypesense
      .createCollectionFlow(settings)
      .mapMaterializedValue(_.toJava)
      .asJava

  /**
   * Creates a sink for creating collections.
   */
  def createCollectionSink(settings: TypesenseSettings): Sink[CollectionSchema, CompletionStage[Done]] =
    ScalaTypesense
      .createCollectionSink(settings)
      .mapMaterializedValue(_.toJava)
      .asJava

  /**
   * Retrieve a collection.
   */
  def retrieveCollectionRequest(
      settings: TypesenseSettings,
      retrieve: RetrieveCollection,
      system: ActorSystem
  ): CompletionStage[CollectionResponse] =
    ScalaTypesense
      .retrieveCollectionRequest(settings, retrieve)(system)
      .toJava

  /**
   * Creates a flow for retrieving collections.
   */
  def retrieveCollectionFlow(
      settings: TypesenseSettings
  ): Flow[RetrieveCollection, CollectionResponse, CompletionStage[NotUsed]] =
    ScalaTypesense
      .retrieveCollectionFlow(settings)
      .mapMaterializedValue(_.toJava)
      .asJava

  /**
   * Index a single document.
   */
  def indexDocumentRequest[T](
      settings: TypesenseSettings,
      document: IndexDocument[T],
      system: ActorSystem,
      jsonWriter: JsonWriter[T]
  ): CompletionStage[Done] =
    ScalaTypesense
      .indexDocumentRequest(settings, document)(jsonWriter, system)
      .toJava

  /**
   * Creates a flow for indexing a single document.
   */
  def indexDocumentFlow[T](
      settings: TypesenseSettings,
      jsonWriter: JsonWriter[T]
  ): Flow[IndexDocument[T], Done, CompletionStage[NotUsed]] =
    ScalaTypesense
      .indexDocumentFlow(settings)(jsonWriter)
      .mapMaterializedValue(_.toJava)
      .asJava

  /**
   * Creates a sink for indexing a single document.
   */
  def indexDocumentSink[T](
      settings: TypesenseSettings,
      jsonWriter: JsonWriter[T]
  ): Sink[IndexDocument[T], CompletionStage[Done]] =
    ScalaTypesense
      .indexDocumentSink[T](settings)(jsonWriter)
      .mapMaterializedValue(_.toJava)
      .asJava

  /**
   * Index many documents.
   */
  def indexManyDocumentRequest[T](
      settings: TypesenseSettings,
      index: IndexManyDocuments[T],
      system: ActorSystem,
      jsonWriter: JsonWriter[T]
  ): CompletionStage[java.util.List[IndexDocumentResult]] =
    ScalaTypesense
      .indexManyDocumentsRequest(settings, index)(jsonWriter, system)
      .map(_.asJava)(system.dispatcher)
      .toJava

  /**
   * Creates a flow for indexing many documents.
   */
  def indexManyDocumentsFlow[T](
      settings: TypesenseSettings,
      jsonWriter: JsonWriter[T]
  ): Flow[IndexManyDocuments[T], java.util.List[IndexDocumentResult], CompletionStage[NotUsed]] =
    ScalaTypesense
      .indexManyDocumentsFlow(settings)(jsonWriter)
      .mapMaterializedValue(_.toJava)
      .map(_.asJava)
      .asJava

  /**
   * Retrieve a document.
   */
  def retrieveDocumentRequest[T](
      settings: TypesenseSettings,
      retrieve: RetrieveDocument,
      system: ActorSystem,
      jsonReader: JsonReader[T]
  ): CompletionStage[T] =
    ScalaTypesense
      .retrieveDocumentRequest(settings, retrieve)(jsonReader, system)
      .toJava

  /**
   * Creates a flow for retrieving documents.
   */
  def retrieveDocumentFlow[T](
      settings: TypesenseSettings,
      jsonReader: JsonReader[T]
  ): Flow[RetrieveDocument, T, CompletionStage[NotUsed]] =
    ScalaTypesense
      .retrieveDocumentFlow(settings)(jsonReader)
      .mapMaterializedValue(_.toJava)
      .asJava

  /**
   * Delete a single document.
   */
  def deleteDocumentRequest[T](
      settings: TypesenseSettings,
      delete: DeleteDocument,
      system: ActorSystem
  ): CompletionStage[Done] =
    ScalaTypesense
      .deleteDocumentRequest(settings, delete)(system)
      .toJava

  /**
   * Creates a flow for deleting a single document.
   */
  def deleteDocumentFlow(
      settings: TypesenseSettings
  ): Flow[DeleteDocument, Done, CompletionStage[NotUsed]] =
    ScalaTypesense
      .deleteDocumentFlow(settings)
      .mapMaterializedValue(_.toJava)
      .asJava

  /**
   * Creates a sink for deleting single document.
   */
  def deleteDocumentSink(
      settings: TypesenseSettings
  ): Sink[DeleteDocument, CompletionStage[Done]] =
    ScalaTypesense
      .deleteDocumentSink(settings)
      .mapMaterializedValue(_.toJava)
      .asJava
}
