/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
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

object SolrSink {

  /**
   * Java API: creates a [[SolrFlow]] to Solr for sequence of [[WriteMessage]] containing [[SolrInputDocument]].
   */
  def documents(
      collection: String,
      settings: SolrUpdateSettings,
      client: SolrClient
  ): javadsl.Sink[JavaList[WriteMessage[SolrInputDocument, NotUsed]], CompletionStage[Done]] =
    SolrFlow
      .documents(collection, settings, client)
      .toMat(javadsl.Sink.ignore[java.util.List[WriteResult[SolrInputDocument, NotUsed]]],
             javadsl.Keep.right[NotUsed, CompletionStage[Done]])

  /**
   * Java API: creates a [[SolrFlow]] to Solr for sequence of [[WriteMessage]] containing type `T`
   * with [[org.apache.solr.client.solrj.beans.DocumentObjectBinder]].
   */
  def beans[T](
      collection: String,
      settings: SolrUpdateSettings,
      client: SolrClient,
      clazz: Class[T]
  ): Sink[JavaList[WriteMessage[T, NotUsed]], CompletionStage[Done]] =
    SolrFlow
      .beans[T](collection, settings, client, clazz)
      .toMat(javadsl.Sink.ignore[java.util.List[WriteResult[T, NotUsed]]],
             javadsl.Keep.right[NotUsed, CompletionStage[Done]])

  /**
   * Java API: creates a [[SolrFlow]] to Solr for sequence of [[WriteMessage]] containing type `T` with `binder` of type 'T'.
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
      .toMat(javadsl.Sink.ignore[java.util.List[WriteResult[T, NotUsed]]],
             javadsl.Keep.right[NotUsed, CompletionStage[Done]])
}
