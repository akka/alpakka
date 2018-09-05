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
   * @deprecated ("use the method documents to batch operation")
   */
  def document(
      collection: String,
      settings: SolrUpdateSettings
  )(implicit client: SolrClient): Flow[IncomingMessage[SolrInputDocument, NotUsed], Seq[
    IncomingMessageResult[SolrInputDocument, NotUsed]
  ], NotUsed] =
    Flow
      .fromFunction[IncomingMessage[SolrInputDocument, NotUsed], Seq[IncomingMessage[SolrInputDocument, NotUsed]]](
        t => Seq(t)
      )
      .via(
        documents(collection, settings)(client)
      )

  /**
   * Scala API: creates a [[SolrFlowStage]] for [[SolrInputDocument]] from sequences of [[IncomingMessage]] to sequences
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
          identity
        )
      )

  /**
   * Scala API: creates a [[SolrFlowStage]] for type `T` from [[IncomingMessage]] to sequence
   * of [[IncomingMessageResult]] with [[org.apache.solr.client.solrj.beans.DocumentObjectBinder]].
   * @deprecated ("use the method beans to batch operation")
   */
  def bean[T](
      collection: String,
      settings: SolrUpdateSettings
  )(
      implicit client: SolrClient
  ): Flow[IncomingMessage[T, NotUsed], Seq[IncomingMessageResult[T, NotUsed]], NotUsed] =
    Flow
      .fromFunction[IncomingMessage[T, NotUsed], Seq[IncomingMessage[T, NotUsed]]](
        t => Seq(t)
      )
      .via(
        beans[T](collection, settings)(client)
      )

  /**
   * Scala API: creates a [[SolrFlowStage]] for type `T` from sequence of [[IncomingMessage]] to sequences
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
          new DefaultSolrObjectBinder
        )
      )

  /**
   * Scala API: creates a [[SolrFlowStage]] for type `T` from [[IncomingMessage]] to sequence
   * of [[IncomingMessageResult]] with `binder` of type 'T'.
   * @deprecated ("use the method typeds to batch operation")
   */
  def typed[T](
      collection: String,
      settings: SolrUpdateSettings,
      binder: T => SolrInputDocument
  )(
      implicit client: SolrClient
  ): Flow[IncomingMessage[T, NotUsed], Seq[IncomingMessageResult[T, NotUsed]], NotUsed] =
    Flow
      .fromFunction[IncomingMessage[T, NotUsed], Seq[IncomingMessage[T, NotUsed]]](
        t => Seq(t)
      )
      .via(
        typeds[T](collection, settings, binder)(client)
      )

  /**
   * Scala API: creates a [[SolrFlowStage]] for type `T` from sequence of [[IncomingMessage]] to sequences
   * of [[IncomingMessageResult]] with `binder` of type 'T'.
   */
  def typeds[T](
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
          binder
        )
      )

  /**
   * Scala API: creates a [[SolrFlowStage]] for [[SolrInputDocument]] from [[IncomingMessage]] to sequences
   * of [[IncomingMessageResult]] with `passThrough` of type `C`.
   * @deprecated ("use the method documentsWithPassThrough to batch operation")
   */
  def documentWithPassThrough[C](
      collection: String,
      settings: SolrUpdateSettings
  )(
      implicit client: SolrClient
  ): Flow[IncomingMessage[SolrInputDocument, C], Seq[IncomingMessageResult[SolrInputDocument, C]], NotUsed] =
    Flow
      .fromFunction[IncomingMessage[SolrInputDocument, C], Seq[IncomingMessage[SolrInputDocument, C]]](
        t => Seq(t)
      )
      .via(
        documentsWithPassThrough[C](collection, settings)(client)
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
          identity
        )
      )

  /**
   * Scala API: creates a [[SolrFlowStage]] for type `T` from [[IncomingMessage]] to sequence
   * of [[IncomingMessageResult]] with `passThrough` of type `C`
   * with [[org.apache.solr.client.solrj.beans.DocumentObjectBinder]].
   * @deprecated ("use the method beansWithPassThrough to batch operation")
   */
  def beanWithPassThrough[T, C](
      collection: String,
      settings: SolrUpdateSettings
  )(
      implicit client: SolrClient
  ): Flow[IncomingMessage[T, C], Seq[IncomingMessageResult[T, C]], NotUsed] =
    Flow
      .fromFunction[IncomingMessage[T, C], Seq[IncomingMessage[T, C]]](
        t => Seq(t)
      )
      .via(
        beansWithPassThrough(collection, settings)(client)
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
          new DefaultSolrObjectBinder
        )
      )

  /**
   * Scala API: creates a [[SolrFlowStage]] for type `T` from [[IncomingMessage]] to sequence
   * of [[IncomingMessageResult]] with `passThrough` of type `C` and `binder` of type 'T'.
   * @deprecated ("use the method typedsWithPassThrough to batch operation")
   */
  def typedWithPassThrough[T, C](
      collection: String,
      settings: SolrUpdateSettings,
      binder: T => SolrInputDocument
  )(
      implicit client: SolrClient
  ): Flow[IncomingMessage[T, C], Seq[IncomingMessageResult[T, C]], NotUsed] =
    Flow
      .fromFunction[IncomingMessage[T, C], Seq[IncomingMessage[T, C]]](
        t => Seq(t)
      )
      .via(
        typedsWithPassThrough(collection, settings, binder)(client)
      )

  /**
   * Scala API: creates a [[SolrFlowStage]] for type 'T' from [[IncomingMessage]]
   * to lists of [[IncomingMessageResult]] with `passThrough` of type `C` and `binder` of type `T`.
   */
  def typedsWithPassThrough[T, C](
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
          binder
        )
      )

  private class DefaultSolrObjectBinder(implicit c: SolrClient) extends (Any => SolrInputDocument) {
    override def apply(v1: Any): SolrInputDocument =
      c.getBinder.toSolrInputDocument(v1)
  }

}
