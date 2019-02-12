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

import scala.collection.JavaConverters._

object SolrFlow {

  /**
   * Java API: creates a [[SolrFlowStage]] for [[SolrInputDocument]]
   * from [[WriteMessage]] to sequences of [[WriteResult]].
   *
   * @deprecated ("use the method documents to batch operation","0.20")
   */
  def document(
      collection: String,
      settings: SolrUpdateSettings,
      client: SolrClient
  ): javadsl.Flow[WriteMessage[SolrInputDocument, NotUsed], JavaList[
    WriteResult[SolrInputDocument, NotUsed]
  ], NotUsed] =
    ScalaSolrFlow
      .document(collection, settings)(client)
      .map {
        _.asJava
      }
      .asJava

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
      .fromFunction[JavaList[WriteMessage[SolrInputDocument, NotUsed]], Seq[WriteMessage[SolrInputDocument, NotUsed]]](
        _.asScala.toSeq
      )
      .via(
        ScalaSolrFlow
          .documents(collection, settings)(client)
      )
      .map(_.asJava)
      .asJava

  /**
   * Java API: creates a [[SolrFlowStage]] for type `T` from [[WriteMessage]] to sequences
   * of [[WriteResult]] with [[org.apache.solr.client.solrj.beans.DocumentObjectBinder]].
   *
   * @deprecated ("use the method beans to batch operation","0.20")
   */
  def bean[T](
      collection: String,
      settings: SolrUpdateSettings,
      client: SolrClient,
      clazz: Class[T]
  ): javadsl.Flow[WriteMessage[T, NotUsed], JavaList[WriteResult[T, NotUsed]], NotUsed] =
    ScalaSolrFlow
      .bean[T](collection, settings)(client)
      .map {
        _.asJava
      }
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
      .fromFunction[JavaList[WriteMessage[T, NotUsed]], Seq[WriteMessage[T, NotUsed]]](_.asScala.toSeq)
      .via(
        ScalaSolrFlow
          .beans[T](collection, settings)(client)
      )
      .map(_.asJava)
      .asJava

  /**
   * Java API: creates a [[SolrFlowStage]] for type `T` from [[WriteMessage]] to sequences
   * of [[WriteResult]] with `binder` of type 'T'.
   *
   * @deprecated ("use the method typeds to batch operation","0.20")
   */
  def typed[T](
      collection: String,
      settings: SolrUpdateSettings,
      binder: Function[T, SolrInputDocument],
      client: SolrClient,
      clazz: Class[T]
  ): javadsl.Flow[WriteMessage[T, NotUsed], JavaList[WriteResult[T, NotUsed]], NotUsed] =
    ScalaSolrFlow
      .typed[T](collection, settings, i => binder.apply(i))(client)
      .map {
        _.asJava
      }
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
      .fromFunction[JavaList[WriteMessage[T, NotUsed]], Seq[WriteMessage[T, NotUsed]]](_.asScala.toSeq)
      .via(
        ScalaSolrFlow
          .typeds[T](collection, settings, i => binder.apply(i))(client)
      )
      .map(_.asJava)
      .asJava

  /**
   * Java API: creates a [[SolrFlowStage]] for [[SolrInputDocument]]
   * from [[WriteMessage]] to sequences of [[WriteResult]] with `passThrough` of type `C`.
   *
   * @deprecated ("use the method documentsWithPassThrough to batch operation","0.20")
   */
  def documentWithPassThrough[C](
      collection: String,
      settings: SolrUpdateSettings,
      client: SolrClient
  ): javadsl.Flow[WriteMessage[SolrInputDocument, C], JavaList[WriteResult[SolrInputDocument, C]], NotUsed] =
    ScalaSolrFlow
      .documentWithPassThrough[C](collection, settings)(client)
      .map {
        _.asJava
      }
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
      .fromFunction[JavaList[WriteMessage[SolrInputDocument, C]], Seq[WriteMessage[SolrInputDocument, C]]](
        _.asScala.toSeq
      )
      .via(
        ScalaSolrFlow
          .documentsWithPassThrough(collection, settings)(client)
      )
      .map(_.asJava)
      .asJava

  /**
   * Java API: creates a [[SolrFlowStage]] for type `T` from [[WriteMessage]] to sequences
   * of [[WriteResult]] with `passThrough` of type `C` and [[org.apache.solr.client.solrj.beans.DocumentObjectBinder]].
   *
   * @deprecated ("use the method beansWithPassThrough to batch operation","0.20")
   */
  def beanWithPassThrough[T, C](
      collection: String,
      settings: SolrUpdateSettings,
      client: SolrClient,
      clazz: Class[T]
  ): javadsl.Flow[WriteMessage[T, C], JavaList[WriteResult[T, C]], NotUsed] =
    ScalaSolrFlow
      .beanWithPassThrough[T, C](collection, settings)(client)
      .map {
        _.asJava
      }
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
      .fromFunction[JavaList[WriteMessage[T, C]], Seq[WriteMessage[T, C]]](_.asScala.toSeq)
      .via(
        ScalaSolrFlow
          .beansWithPassThrough[T, C](collection, settings)(client)
      )
      .map(_.asJava)
      .asJava

  /**
   * Java API: creates a [[SolrFlowStage]] for type `T` from [[WriteMessage]] to sequences
   * of [[WriteResult]] with `binder` of type 'T'.
   *
   * @deprecated ("use the method typedsWithPassThrough to batch operation","0.20")
   */
  def typedWithPassThrough[T, C](
      collection: String,
      settings: SolrUpdateSettings,
      binder: Function[T, SolrInputDocument],
      client: SolrClient,
      clazz: Class[T]
  ): javadsl.Flow[WriteMessage[T, C], JavaList[WriteResult[T, C]], NotUsed] =
    ScalaSolrFlow
      .typedWithPassThrough[T, C](collection, settings, i => binder.apply(i))(client)
      .map {
        _.asJava
      }
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
      .fromFunction[JavaList[WriteMessage[T, C]], Seq[WriteMessage[T, C]]](_.asScala.toSeq)
      .via(
        ScalaSolrFlow
          .typedsWithPassThrough[T, C](collection, settings, i => binder.apply(i))(client)
      )
      .map(_.asJava)
      .asJava

}
