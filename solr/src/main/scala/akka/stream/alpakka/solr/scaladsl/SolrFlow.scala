/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.solr.scaladsl

import akka.NotUsed
import akka.stream.alpakka.solr.{IncomingMessage, IncomingMessageResult, SolrFlowStage, SolrUpdateSettings}
import akka.stream.scaladsl.Flow
import org.apache.solr.client.solrj.SolrClient
import org.apache.solr.common.SolrInputDocument

object SolrFlow {

  /**
   * Scala API: creates a [[SolrFlowStage]] for [[SolrInputDocument]] from [[IncomingMessage]] to sequences
   * of [[IncomingMessageResult]].
   */
  def documents(
      collection: String,
      settings: SolrUpdateSettings
  )(implicit client: SolrClient): Flow[Seq[IncomingMessage[SolrInputDocument, NotUsed]], Seq[
    IncomingMessageResult[SolrInputDocument, NotUsed]
  ], NotUsed] =
    Flow
      .fromGraph(
        new SolrFlowStage[SolrInputDocument, NotUsed](
          collection,
          client,
          settings,
          Some(identity)
        )
      )

  /**
   * Scala API: creates a [[SolrFlowStage]] for type `T` from [[IncomingMessage]] to sequences
   * of [[IncomingMessageResult]] with [[org.apache.solr.client.solrj.beans.DocumentObjectBinder]].
   */
  def beans[T](
      collection: String,
      settings: SolrUpdateSettings
  )(
      implicit client: SolrClient
  ): Flow[Seq[IncomingMessage[T, NotUsed]], Seq[IncomingMessageResult[T, NotUsed]], NotUsed] =
    Flow
      .fromGraph(
        new SolrFlowStage[T, NotUsed](
          collection,
          client,
          settings,
          Some(new DefaultSolrObjectBinder)
        )
      )

  /**
   * Scala API: creates a [[SolrFlowStage]] for type `T` from [[IncomingMessage]] to sequences
   * of [[IncomingMessageResult]] with `binder` of type 'T'.
   */
  def typed[T](
      collection: String,
      settings: SolrUpdateSettings,
      binder: T => SolrInputDocument
  )(
      implicit client: SolrClient
  ): Flow[Seq[IncomingMessage[T, NotUsed]], Seq[IncomingMessageResult[T, NotUsed]], NotUsed] =
    Flow
      .fromGraph(
        new SolrFlowStage[T, NotUsed](
          collection,
          client,
          settings,
          Some(binder)
        )
      )

  /**
   * Scala API: creates a [[SolrFlowStage]] for message to delete from [[IncomingMessage]] to sequences
   * of [[IncomingMessageResult]].
   */
  def deletes(collection: String, settings: SolrUpdateSettings)(
      implicit client: SolrClient
  ): Flow[Seq[IncomingMessage[NotUsed, NotUsed]], Seq[
    IncomingMessageResult[NotUsed, NotUsed]
  ], NotUsed] =
    Flow
      .fromGraph(
        new SolrFlowStage[NotUsed, NotUsed](
          collection,
          client,
          settings,
          None
        )
      )

  /**
   * Scala API: creates a [[SolrFlowStage]] for message to atomically update from [[IncomingMessage]] to sequences
   * of [[IncomingMessageResult]].
   */
  def updates(collection: String, settings: SolrUpdateSettings)(
      implicit client: SolrClient
  ): Flow[Seq[IncomingMessage[NotUsed, NotUsed]], Seq[
    IncomingMessageResult[NotUsed, NotUsed]
  ], NotUsed] =
    Flow
      .fromGraph(
        new SolrFlowStage[NotUsed, NotUsed](
          collection,
          client,
          settings,
          None
        )
      )

  /**
   * Scala API: creates a [[SolrFlowStage]] for [[SolrInputDocument]] from [[IncomingMessage]]
   * to lists of [[IncomingMessageResult]] with `passThrough` of type `C`.
   */
  def documentsWithPassThrough[C](
      collection: String,
      settings: SolrUpdateSettings
  )(
      implicit client: SolrClient
  ): Flow[Seq[IncomingMessage[SolrInputDocument, C]], Seq[IncomingMessageResult[SolrInputDocument, C]], NotUsed] =
    Flow
      .fromGraph(
        new SolrFlowStage[SolrInputDocument, C](
          collection,
          client,
          settings,
          Some(identity)
        )
      )

  /**
   * Scala API: creates a [[SolrFlowStage]] for type 'T' from [[IncomingMessage]]
   * to lists of [[IncomingMessageResult]] with `passThrough` of type `C`
   * and [[org.apache.solr.client.solrj.beans.DocumentObjectBinder]] for type 'T' .
   */
  def beansWithPassThrough[T, C](
      collection: String,
      settings: SolrUpdateSettings
  )(implicit client: SolrClient): Flow[Seq[IncomingMessage[T, C]], Seq[IncomingMessageResult[T, C]], NotUsed] =
    Flow
      .fromGraph(
        new SolrFlowStage[T, C](
          collection,
          client,
          settings,
          Some(new DefaultSolrObjectBinder)
        )
      )

  /**
   * Scala API: creates a [[SolrFlowStage]] for type 'T' from [[IncomingMessage]]
   * to lists of [[IncomingMessageResult]] with `passThrough` of type `C` and `binder` of type `T`.
   */
  def typedWithPassThrough[T, C](
      collection: String,
      settings: SolrUpdateSettings,
      binder: T => SolrInputDocument
  )(implicit client: SolrClient): Flow[Seq[IncomingMessage[T, C]], Seq[IncomingMessageResult[T, C]], NotUsed] =
    Flow
      .fromGraph(
        new SolrFlowStage[T, C](
          collection,
          client,
          settings,
          Some(binder)
        )
      )

  /**
   * Scala API: creates a [[SolrFlowStage]] for message to delete from [[IncomingMessage]] to sequences
   * of [[IncomingMessageResult]] with `passThrough` of type `C`.
   */
  def deletesWithPassThrough[C](collection: String, settings: SolrUpdateSettings)(
      implicit client: SolrClient
  ): Flow[Seq[IncomingMessage[NotUsed, C]], Seq[
    IncomingMessageResult[NotUsed, C]
  ], NotUsed] =
    Flow
      .fromGraph(
        new SolrFlowStage[NotUsed, C](
          collection,
          client,
          settings,
          None
        )
      )

  /**
   * Scala API: creates a [[SolrFlowStage]] for message to atomically update from [[IncomingMessage]] to sequences
   * of [[IncomingMessageResult]] with `passThrough` of type `C`.
   */
  def updatesWithPassThrough[C](collection: String, settings: SolrUpdateSettings)(
      implicit client: SolrClient
  ): Flow[Seq[IncomingMessage[NotUsed, C]], Seq[
    IncomingMessageResult[NotUsed, C]
  ], NotUsed] =
    Flow
      .fromGraph(
        new SolrFlowStage[NotUsed, C](
          collection,
          client,
          settings,
          None
        )
      )

  private class DefaultSolrObjectBinder(implicit c: SolrClient) extends (Any => SolrInputDocument) {
    override def apply(v1: Any): SolrInputDocument =
      c.getBinder.toSolrInputDocument(v1)
  }

}
