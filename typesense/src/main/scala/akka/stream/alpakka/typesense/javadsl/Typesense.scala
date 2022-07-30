/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.typesense.javadsl

import akka.stream.alpakka.typesense._
import akka.stream.javadsl.Flow
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
   * Creates a flow for creating collections.
   */
  def createCollectionFlow(
      settings: TypesenseSettings
  ): Flow[CollectionSchema, TypesenseResult[CollectionResponse], CompletionStage[NotUsed]] =
    ScalaTypesense
      .createCollectionFlow(settings)
      .mapMaterializedValue(_.toJava)
      .asJava

  /**
   * Creates a flow for retrieving collections.
   */
  def retrieveCollectionFlow(
      settings: TypesenseSettings
  ): Flow[RetrieveCollection, TypesenseResult[CollectionResponse], CompletionStage[NotUsed]] =
    ScalaTypesense
      .retrieveCollectionFlow(settings)
      .mapMaterializedValue(_.toJava)
      .asJava

  /**
   * Creates a flow for indexing a single document.
   */
  def indexDocumentFlow[T](
      settings: TypesenseSettings,
      jsonWriter: JsonWriter[T]
  ): Flow[IndexDocument[T], TypesenseResult[Done], CompletionStage[NotUsed]] =
    ScalaTypesense
      .indexDocumentFlow(settings)(jsonWriter)
      .mapMaterializedValue(_.toJava)
      .asJava

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
   * Creates a flow for retrieving documents.
   */
  def retrieveDocumentFlow[T](
      settings: TypesenseSettings,
      jsonReader: JsonReader[T]
  ): Flow[RetrieveDocument, TypesenseResult[T], CompletionStage[NotUsed]] =
    ScalaTypesense
      .retrieveDocumentFlow(settings)(jsonReader)
      .mapMaterializedValue(_.toJava)
      .asJava

  /**
   * Creates a flow for deleting a single document.
   */
  def deleteDocumentFlow(
      settings: TypesenseSettings
  ): Flow[DeleteDocument, TypesenseResult[Done], CompletionStage[NotUsed]] =
    ScalaTypesense
      .deleteDocumentFlow(settings)
      .mapMaterializedValue(_.toJava)
      .asJava

  /**
   * Creates a flow for deleting many documents by flow.
   */
  def deleteManyDocumentsByQueryFlow(
      settings: TypesenseSettings
  ): Flow[DeleteManyDocumentsByQuery, DeleteManyDocumentsResult, CompletionStage[NotUsed]] =
    ScalaTypesense
      .deleteManyDocumentsByQueryFlow(settings)
      .mapMaterializedValue(_.toJava)
      .asJava
}
