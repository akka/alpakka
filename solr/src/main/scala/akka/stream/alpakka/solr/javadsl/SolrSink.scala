/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.solr.javadsl

import java.util.concurrent.CompletionStage
import java.util.function.Function

import akka.stream.alpakka.solr.{IncomingMessage, SolrUpdateSettings}
import akka.stream.javadsl
import akka.stream.javadsl.Sink
import akka.{Done, NotUsed}
import org.apache.solr.client.solrj.SolrClient
import org.apache.solr.common.SolrInputDocument

object SolrSink {

  /**
   * Java API: creates a [[SolrFlow] to Solr for [[IncomingMessage]] containing [[SolrInputDocument]].
   */
  def document(
      collection: String,
      settings: SolrUpdateSettings,
      client: SolrClient
  ): javadsl.Sink[IncomingMessage[SolrInputDocument, NotUsed], CompletionStage[Done]] =
    SolrFlow
      .document(collection, settings, client)
      .toMat(javadsl.Sink.ignore, javadsl.Keep.right[NotUsed, CompletionStage[Done]])

  /**
   * Java API: creates a [[SolrFlow] to Solr for [[IncomingMessage]] containing type `T`
   * with [[org.apache.solr.client.solrj.beans.DocumentObjectBinder]].
   */
  def bean[T](
      collection: String,
      settings: SolrUpdateSettings,
      client: SolrClient,
      clazz: Class[T]
  ): Sink[IncomingMessage[T, NotUsed], CompletionStage[Done]] =
    SolrFlow
      .bean[T](collection, settings, client, clazz)
      .toMat(javadsl.Sink.ignore, javadsl.Keep.right[NotUsed, CompletionStage[Done]])

  /**
   * Java API: creates a [[SolrFlow] to Solr for [[IncomingMessage]] containing type `T` with `binder` of type 'T'.
   */
  def typed[T](
      collection: String,
      settings: SolrUpdateSettings,
      binder: Function[T, SolrInputDocument],
      client: SolrClient,
      clazz: Class[T]
  ): javadsl.Sink[IncomingMessage[T, NotUsed], CompletionStage[Done]] =
    SolrFlow
      .typed[T](collection, settings, binder, client, clazz)
      .toMat(javadsl.Sink.ignore, javadsl.Keep.right[NotUsed, CompletionStage[Done]])
}
