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
  def documents[T](collection: String, settings: SolrUpdateSettings)(
      implicit client: SolrClient
  ): Sink[Seq[IncomingMessage[SolrInputDocument, NotUsed]], Future[Done]] =
    SolrFlow
      .documents(collection, settings)
      .toMat(Sink.ignore)(Keep.right)

  /**
   * Scala API: creates a [[SolrFlow] to Solr for [[IncomingMessage]] containing type `T`
   * with [[org.apache.solr.client.solrj.beans.DocumentObjectBinder]].
   */
  def beans[T](collection: String, settings: SolrUpdateSettings)(
      implicit client: SolrClient
  ): Sink[Seq[IncomingMessage[T, NotUsed]], Future[Done]] =
    SolrFlow
      .beans[T](collection, settings)
      .toMat(Sink.ignore)(Keep.right)

  /**
   * Scala API: creates a [[SolrFlow] to Solr for [[IncomingMessage]] containing type `T` with `binder` of type 'T'.
   */
  def typed[T](
      collection: String,
      settings: SolrUpdateSettings,
      binder: T => SolrInputDocument
  )(implicit client: SolrClient): Sink[Seq[IncomingMessage[T, NotUsed]], Future[Done]] =
    SolrFlow
      .typed[T](collection, settings, binder)
      .toMat(Sink.ignore)(Keep.right)

  /**
   * Scala API: creates a [[SolrFlow] to Solr for [[IncomingMessage]] containing ids to delete.
   */
  def deletes[T](collection: String, settings: SolrUpdateSettings)(
      implicit client: SolrClient
  ): Sink[Seq[IncomingMessage[NotUsed, NotUsed]], Future[Done]] =
    SolrFlow
      .deletes(collection, settings)
      .toMat(Sink.ignore)(Keep.right)

  /**
   * Scala API: creates a [[SolrFlow] to Solr for [[IncomingMessage]] containing ids to update.
   */
  def updates[T](collection: String, settings: SolrUpdateSettings)(
      implicit client: SolrClient
  ): Sink[Seq[IncomingMessage[NotUsed, NotUsed]], Future[Done]] =
    SolrFlow
      .updates(collection, settings)
      .toMat(Sink.ignore)(Keep.right)
}
