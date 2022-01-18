/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.solr.javadsl

import java.util.concurrent.CompletionStage
import java.util.function.Function

import akka.stream.alpakka.solr.{SolrUpdateSettings, WriteMessage, WriteResult}
import akka.stream.javadsl
import akka.stream.javadsl.Sink
import akka.{Done, NotUsed}
import org.apache.solr.client.solrj.SolrClient
import org.apache.solr.common.SolrInputDocument
import java.util.{List => JavaList}

/**
 * Java API
 */
object SolrSink {

  /**
   * Write `SolrInputDocument`s to Solr.
   */
  def documents(
      collection: String,
      settings: SolrUpdateSettings,
      client: SolrClient
  ): javadsl.Sink[JavaList[WriteMessage[SolrInputDocument, NotUsed]], CompletionStage[Done]] =
    SolrFlow
      .documents(collection, settings, client)
      .toMat(javadsl.Sink.ignore[java.util.List[WriteResult[SolrInputDocument, NotUsed]]](),
             javadsl.Keep.right[NotUsed, CompletionStage[Done]])

  /**
   * Write Java bean stream elements to Solr.
   * The stream element classes must be annotated for use with [[org.apache.solr.client.solrj.beans.DocumentObjectBinder]] for conversion.
   */
  def beans[T](
      collection: String,
      settings: SolrUpdateSettings,
      client: SolrClient,
      clazz: Class[T]
  ): Sink[JavaList[WriteMessage[T, NotUsed]], CompletionStage[Done]] =
    SolrFlow
      .beans[T](collection, settings, client, clazz)
      .toMat(javadsl.Sink.ignore[java.util.List[WriteResult[T, NotUsed]]](),
             javadsl.Keep.right[NotUsed, CompletionStage[Done]])

  /**
   * Write stream elements to Solr.
   *
   * @param binder a conversion function to create `SolrInputDocument`s of the stream elements
   */
  def typeds[T](
      collection: String,
      settings: SolrUpdateSettings,
      binder: Function[T, SolrInputDocument],
      client: SolrClient,
      clazz: Class[T]
  ): javadsl.Sink[JavaList[WriteMessage[T, NotUsed]], CompletionStage[Done]] =
    SolrFlow
      .typeds[T](collection, settings, binder, client, clazz)
      .toMat(javadsl.Sink.ignore[java.util.List[WriteResult[T, NotUsed]]](),
             javadsl.Keep.right[NotUsed, CompletionStage[Done]])
}
