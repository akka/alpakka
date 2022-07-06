/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.typesense.javadsl

import akka.actor.ActorSystem
import akka.stream.alpakka.typesense.{
  CollectionResponse,
  CollectionSchema,
  IndexDocument,
  RetrieveCollection,
  TypesenseSettings
}
import akka.stream.javadsl.{Flow, Sink}
import akka.{Done, NotUsed}
import spray.json.{JsonReader, JsonWriter}

import java.util.concurrent.CompletionStage
import scala.compat.java8.FutureConverters.FutureOps

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
}
