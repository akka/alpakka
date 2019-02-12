/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.solr.scaladsl

import akka.NotUsed
import akka.stream.alpakka.solr.impl.SolrFlowStage
import akka.stream.alpakka.solr.{SolrUpdateSettings, WriteMessage, WriteResult}
import akka.stream.scaladsl.Flow
import org.apache.solr.client.solrj.SolrClient
import org.apache.solr.common.SolrInputDocument

import scala.collection.immutable

object SolrFlow {

  /**
   * Scala API: creates a [[SolrFlowStage]] for [[SolrInputDocument]] from [[WriteMessage]] to sequences
   * of [[WriteResult]].
   *
   * @deprecated ("use the method documents to batch operation","0.20")
   */
  def document(
      collection: String,
      settings: SolrUpdateSettings
  )(
      implicit client: SolrClient
  ): Flow[WriteMessage[SolrInputDocument, NotUsed], immutable.Seq[WriteResult[SolrInputDocument, NotUsed]], NotUsed] =
    Flow
      .fromFunction[WriteMessage[SolrInputDocument, NotUsed], immutable.Seq[WriteMessage[SolrInputDocument, NotUsed]]](
        t => immutable.Seq(t)
      )
      .via(
        documents(collection, settings)(client)
      )

  /**
   * Scala API: creates a [[SolrFlowStage]] for [[SolrInputDocument]] from sequences of [[WriteMessage]] to sequences
   * of [[WriteResult]].
   */
  def documents(
      collection: String,
      settings: SolrUpdateSettings
  )(
      implicit client: SolrClient
  ): Flow[immutable.Seq[WriteMessage[SolrInputDocument, NotUsed]], immutable.Seq[WriteResult[SolrInputDocument,
                                                                                             NotUsed]], NotUsed] =
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
   * Scala API: creates a [[SolrFlowStage]] for type `T` from [[WriteMessage]] to sequence
   * of [[WriteResult]] with [[org.apache.solr.client.solrj.beans.DocumentObjectBinder]].
   *
   * @deprecated ("use the method beans to batch operation","0.20")
   */
  def bean[T](
      collection: String,
      settings: SolrUpdateSettings
  )(
      implicit client: SolrClient
  ): Flow[WriteMessage[T, NotUsed], immutable.Seq[WriteResult[T, NotUsed]], NotUsed] =
    Flow
      .fromFunction[WriteMessage[T, NotUsed], immutable.Seq[WriteMessage[T, NotUsed]]](
        t => immutable.Seq(t)
      )
      .via(
        beans[T](collection, settings)(client)
      )

  /**
   * Scala API: creates a [[SolrFlowStage]] for type `T` from sequence of [[WriteMessage]] to sequences
   * of [[WriteResult]] with [[org.apache.solr.client.solrj.beans.DocumentObjectBinder]].
   */
  def beans[T](
      collection: String,
      settings: SolrUpdateSettings
  )(
      implicit client: SolrClient
  ): Flow[immutable.Seq[WriteMessage[T, NotUsed]], immutable.Seq[WriteResult[T, NotUsed]], NotUsed] =
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
   * Scala API: creates a [[SolrFlowStage]] for type `T` from [[WriteMessage]] to sequence
   * of [[WriteResult]] with `binder` of type 'T'.
   *
   * @deprecated ("use the method typeds to batch operation","0.20")
   */
  def typed[T](
      collection: String,
      settings: SolrUpdateSettings,
      binder: T => SolrInputDocument
  )(
      implicit client: SolrClient
  ): Flow[WriteMessage[T, NotUsed], immutable.Seq[WriteResult[T, NotUsed]], NotUsed] =
    Flow
      .fromFunction[WriteMessage[T, NotUsed], immutable.Seq[WriteMessage[T, NotUsed]]](
        t => immutable.Seq(t)
      )
      .via(
        typeds[T](collection, settings, binder)(client)
      )

  /**
   * Scala API: creates a [[SolrFlowStage]] for type `T` from sequence of [[WriteMessage]] to sequences
   * of [[WriteResult]] with `binder` of type 'T'.
   */
  def typeds[T](
      collection: String,
      settings: SolrUpdateSettings,
      binder: T => SolrInputDocument
  )(
      implicit client: SolrClient
  ): Flow[immutable.Seq[WriteMessage[T, NotUsed]], immutable.Seq[WriteResult[T, NotUsed]], NotUsed] =
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
   * Scala API: creates a [[SolrFlowStage]] for [[SolrInputDocument]] from [[WriteMessage]] to sequences
   * of [[WriteResult]] with `passThrough` of type `C`.
   *
   * @deprecated ("use the method documentsWithPassThrough to batch operation","0.20")
   */
  def documentWithPassThrough[C](
      collection: String,
      settings: SolrUpdateSettings
  )(
      implicit client: SolrClient
  ): Flow[WriteMessage[SolrInputDocument, C], immutable.Seq[WriteResult[SolrInputDocument, C]], NotUsed] =
    Flow
      .fromFunction[WriteMessage[SolrInputDocument, C], immutable.Seq[WriteMessage[SolrInputDocument, C]]](
        t => immutable.Seq(t)
      )
      .via(
        documentsWithPassThrough[C](collection, settings)(client)
      )

  /**
   * Scala API: creates a [[SolrFlowStage]] for [[SolrInputDocument]] from [[WriteMessage]]
   * to lists of [[WriteResult]] with `passThrough` of type `C`.
   */
  def documentsWithPassThrough[C](
      collection: String,
      settings: SolrUpdateSettings
  )(
      implicit client: SolrClient
  ): Flow[immutable.Seq[WriteMessage[SolrInputDocument, C]],
          immutable.Seq[WriteResult[SolrInputDocument, C]],
          NotUsed] =
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
   * Scala API: creates a [[SolrFlowStage]] for type `T` from [[WriteMessage]] to sequence
   * of [[WriteResult]] with `passThrough` of type `C`
   * with [[org.apache.solr.client.solrj.beans.DocumentObjectBinder]].
   *
   * @deprecated ("use the method beansWithPassThrough to batch operation","0.20")
   */
  def beanWithPassThrough[T, C](
      collection: String,
      settings: SolrUpdateSettings
  )(
      implicit client: SolrClient
  ): Flow[WriteMessage[T, C], immutable.Seq[WriteResult[T, C]], NotUsed] =
    Flow
      .fromFunction[WriteMessage[T, C], immutable.Seq[WriteMessage[T, C]]](
        t => immutable.Seq(t)
      )
      .via(
        beansWithPassThrough(collection, settings)(client)
      )

  /**
   * Scala API: creates a [[SolrFlowStage]] for type 'T' from [[WriteMessage]]
   * to lists of [[WriteResult]] with `passThrough` of type `C`
   * and [[org.apache.solr.client.solrj.beans.DocumentObjectBinder]] for type 'T' .
   */
  def beansWithPassThrough[T, C](
      collection: String,
      settings: SolrUpdateSettings
  )(implicit client: SolrClient): Flow[immutable.Seq[WriteMessage[T, C]], immutable.Seq[WriteResult[T, C]], NotUsed] =
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
   * Scala API: creates a [[SolrFlowStage]] for type `T` from [[WriteMessage]] to sequence
   * of [[WriteResult]] with `passThrough` of type `C` and `binder` of type 'T'.
   *
   * @deprecated ("use the method typedsWithPassThrough to batch operation","0.20")
   */
  def typedWithPassThrough[T, C](
      collection: String,
      settings: SolrUpdateSettings,
      binder: T => SolrInputDocument
  )(
      implicit client: SolrClient
  ): Flow[WriteMessage[T, C], immutable.Seq[WriteResult[T, C]], NotUsed] =
    Flow
      .fromFunction[WriteMessage[T, C], immutable.Seq[WriteMessage[T, C]]](
        t => immutable.Seq(t)
      )
      .via(
        typedsWithPassThrough(collection, settings, binder)(client)
      )

  /**
   * Scala API: creates a [[SolrFlowStage]] for type 'T' from [[WriteMessage]]
   * to lists of [[WriteResult]] with `passThrough` of type `C` and `binder` of type `T`.
   */
  def typedsWithPassThrough[T, C](
      collection: String,
      settings: SolrUpdateSettings,
      binder: T => SolrInputDocument
  )(implicit client: SolrClient): Flow[immutable.Seq[WriteMessage[T, C]], immutable.Seq[WriteResult[T, C]], NotUsed] =
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
