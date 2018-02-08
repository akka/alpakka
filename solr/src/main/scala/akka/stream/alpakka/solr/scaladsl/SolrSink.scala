/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.solr.scaladsl

import akka.stream.alpakka.solr.{IncomingMessage, SolrUpdateSettings}
import akka.stream.scaladsl.{Keep, Sink}
import akka.{Done, NotUsed}
import org.apache.solr.client.solrj.SolrClient
import org.apache.solr.common.SolrInputDocument

import scala.concurrent.Future

object SolrSink {

  /**
   * Scala API: creates a [[SolrFlow] to Solr for [[IncomingMessage]] containing [[SolrInputDocument]].
   */
  def document[T](collection: String, settings: SolrUpdateSettings)(
      implicit client: SolrClient
  ): Sink[IncomingMessage[SolrInputDocument, NotUsed], Future[Done]] =
    SolrFlow
      .document(collection, settings)
      .toMat(Sink.ignore)(Keep.right)

  /**
   * Scala API: creates a [[SolrFlow] to Solr for [[IncomingMessage]] containing type `T`
   * with [[org.apache.solr.client.solrj.beans.DocumentObjectBinder]].
   */
  def bean[T](collection: String, settings: SolrUpdateSettings)(
      implicit client: SolrClient
  ): Sink[IncomingMessage[T, NotUsed], Future[Done]] =
    SolrFlow
      .bean[T](collection, settings)
      .toMat(Sink.ignore)(Keep.right)

  /**
   * Scala API: creates a [[SolrFlow] to Solr for [[IncomingMessage]] containing type `T` with `binder` of type 'T'.
   */
  def typed[T](
      collection: String,
      settings: SolrUpdateSettings,
      binder: T => SolrInputDocument
  )(implicit client: SolrClient): Sink[IncomingMessage[T, NotUsed], Future[Done]] =
    SolrFlow
      .typed[T](collection, settings, binder)
      .toMat(Sink.ignore)(Keep.right)
}
