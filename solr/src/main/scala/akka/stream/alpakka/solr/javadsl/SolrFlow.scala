/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.solr.javadsl

import java.util.function.Function
import java.util.{List => JavaList}

import akka.NotUsed
import akka.stream.alpakka.solr.impl.SolrFlowStage
import akka.stream.alpakka.solr.scaladsl.{SolrFlow => ScalaSolrFlow}
import akka.stream.alpakka.solr.{SolrUpdateSettings, WriteMessage, WriteResult}
import akka.stream.javadsl
import akka.stream.scaladsl.Flow
import org.apache.solr.client.solrj.SolrClient
import org.apache.solr.common.SolrInputDocument

import scala.collection.immutable
import scala.collection.JavaConverters._

object SolrFlow {

  /**
   * Java API: creates a [[SolrFlowStage]] for [[SolrInputDocument]]
   * from sequence of [[WriteMessage]] to sequences of [[WriteResult]].
   */
  def documents(
      collection: String,
      settings: SolrUpdateSettings,
      client: SolrClient
  ): javadsl.Flow[JavaList[WriteMessage[SolrInputDocument, NotUsed]], JavaList[
    WriteResult[SolrInputDocument, NotUsed]
  ], NotUsed] =
    Flow
      .fromFunction[JavaList[WriteMessage[SolrInputDocument, NotUsed]], immutable.Seq[WriteMessage[SolrInputDocument,
                                                                                                   NotUsed]]](
        _.asScala.toIndexedSeq
      )
      .via(
        ScalaSolrFlow
          .documents(collection, settings)(client)
      )
      .map(_.asJava)
      .asJava

  /**
   * Java API: creates a [[SolrFlowStage]] for type `T` from sequence of [Seq[IncomingMessage]] to sequences
   * of [[WriteResult]] with [[org.apache.solr.client.solrj.beans.DocumentObjectBinder]].
   */
  def beans[T](
      collection: String,
      settings: SolrUpdateSettings,
      client: SolrClient,
      clazz: Class[T]
  ): javadsl.Flow[JavaList[WriteMessage[T, NotUsed]], JavaList[WriteResult[T, NotUsed]], NotUsed] =
    Flow
      .fromFunction[JavaList[WriteMessage[T, NotUsed]], immutable.Seq[WriteMessage[T, NotUsed]]](_.asScala.toIndexedSeq)
      .via(
        ScalaSolrFlow
          .beans[T](collection, settings)(client)
      )
      .map(_.asJava)
      .asJava

  /**
   * Java API: creates a [[SolrFlowStage]] for type `T` from sequence of [[WriteMessage]] to sequences
   * of [[WriteResult]] with `binder` of type 'T'.
   */
  def typeds[T](
      collection: String,
      settings: SolrUpdateSettings,
      binder: Function[T, SolrInputDocument],
      client: SolrClient,
      clazz: Class[T]
  ): javadsl.Flow[JavaList[WriteMessage[T, NotUsed]], JavaList[WriteResult[T, NotUsed]], NotUsed] =
    Flow
      .fromFunction[JavaList[WriteMessage[T, NotUsed]], immutable.Seq[WriteMessage[T, NotUsed]]](_.asScala.toIndexedSeq)
      .via(
        ScalaSolrFlow
          .typeds[T](collection, settings, i => binder.apply(i))(client)
      )
      .map(_.asJava)
      .asJava

  /**
   * Java API: creates a [[SolrFlowStage]] for [[SolrInputDocument]] from [[WriteMessage]]
   * to lists of [[WriteResult]] with `passThrough` of type `C`.
   */
  def documentsWithPassThrough[C](
      collection: String,
      settings: SolrUpdateSettings,
      client: SolrClient
  ): javadsl.Flow[JavaList[WriteMessage[SolrInputDocument, C]], JavaList[WriteResult[SolrInputDocument, C]], NotUsed] =
    Flow
      .fromFunction[JavaList[WriteMessage[SolrInputDocument, C]], immutable.Seq[WriteMessage[SolrInputDocument, C]]](
        _.asScala.toIndexedSeq
      )
      .via(
        ScalaSolrFlow
          .documentsWithPassThrough(collection, settings)(client)
      )
      .map(_.asJava)
      .asJava

  /**
   * Java API: creates a [[SolrFlowStage]] for type 'T' from sequence of [[WriteMessage]]
   * to lists of [[WriteResult]] with `passThrough` of type `C`
   * and [[org.apache.solr.client.solrj.beans.DocumentObjectBinder]] for type 'T' .
   */
  def beansWithPassThrough[T, C](
      collection: String,
      settings: SolrUpdateSettings,
      client: SolrClient,
      clazz: Class[T]
  ): javadsl.Flow[JavaList[WriteMessage[T, C]], JavaList[WriteResult[T, C]], NotUsed] =
    Flow
      .fromFunction[JavaList[WriteMessage[T, C]], immutable.Seq[WriteMessage[T, C]]](_.asScala.toIndexedSeq)
      .via(
        ScalaSolrFlow
          .beansWithPassThrough[T, C](collection, settings)(client)
      )
      .map(_.asJava)
      .asJava

  /**
   * Java API: creates a [[SolrFlowStage]] for type 'T' from sequence of [[WriteMessage]]
   * to lists of [[WriteResult]] with `passThrough` of type `C` and `binder` of type `T`.
   */
  def typedsWithPassThrough[T, C](
      collection: String,
      settings: SolrUpdateSettings,
      binder: Function[T, SolrInputDocument],
      client: SolrClient,
      clazz: Class[T]
  ): javadsl.Flow[JavaList[WriteMessage[T, C]], JavaList[WriteResult[T, C]], NotUsed] =
    Flow
      .fromFunction[JavaList[WriteMessage[T, C]], immutable.Seq[WriteMessage[T, C]]](_.asScala.toIndexedSeq)
      .via(
        ScalaSolrFlow
          .typedsWithPassThrough[T, C](collection, settings, i => binder.apply(i))(client)
      )
      .map(_.asJava)
      .asJava

}
