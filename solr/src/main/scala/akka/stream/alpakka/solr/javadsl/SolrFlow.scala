/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.solr.javadsl

import java.util.function.Function
import java.util.{List => JavaList}

import akka.NotUsed
import akka.stream.alpakka.solr.scaladsl.{SolrFlow => ScalaSolrFlow}
import akka.stream.alpakka.solr.{IncomingMessage, IncomingMessageResult, SolrUpdateSettings}
import akka.stream.javadsl
import akka.stream.scaladsl.Flow
import org.apache.solr.client.solrj.SolrClient
import org.apache.solr.common.SolrInputDocument

import scala.collection.JavaConverters._

object SolrFlow {

  /**
   * Java API: creates a [[akka.stream.alpakka.solr.SolrFlowStage]] for [[SolrInputDocument]]
   * from [[IncomingMessage]] to sequences of [[IncomingMessageResult]].
   */
  def documents(
      collection: String,
      settings: SolrUpdateSettings,
      client: SolrClient
  ): javadsl.Flow[JavaList[IncomingMessage[SolrInputDocument, NotUsed]], JavaList[
    IncomingMessageResult[SolrInputDocument, NotUsed]
  ], NotUsed] =
    Flow
      .fromFunction[JavaList[IncomingMessage[SolrInputDocument, NotUsed]], Seq[IncomingMessage[SolrInputDocument,
                                                                                               NotUsed]]](
        _.asScala.toSeq
      )
      .via(
        ScalaSolrFlow
          .documents(collection, settings)(client)
      )
      .map(_.asJava)
      .asJava

  /**
   * Java API: creates a [[akka.stream.alpakka.solr.SolrFlowStage]] for type `T` from [[IncomingMessage]] to sequences
   * of [[IncomingMessageResult]] with [[org.apache.solr.client.solrj.beans.DocumentObjectBinder]].
   */
  def beans[T](
      collection: String,
      settings: SolrUpdateSettings,
      client: SolrClient,
      clazz: Class[T]
  ): javadsl.Flow[JavaList[IncomingMessage[T, NotUsed]], JavaList[IncomingMessageResult[T, NotUsed]], NotUsed] =
    Flow
      .fromFunction[JavaList[IncomingMessage[T, NotUsed]], Seq[IncomingMessage[T, NotUsed]]](_.asScala.toSeq)
      .via(
        ScalaSolrFlow
          .beans[T](collection, settings)(client)
      )
      .map(_.asJava)
      .asJava

  /**
   * Java API: creates a [[akka.stream.alpakka.solr.SolrFlowStage]] for type `T` from [[IncomingMessage]] to sequences
   * of [[IncomingMessageResult]] with `binder` of type 'T'.
   */
  def typed[T](
      collection: String,
      settings: SolrUpdateSettings,
      binder: Function[T, SolrInputDocument],
      client: SolrClient,
      clazz: Class[T]
  ): javadsl.Flow[JavaList[IncomingMessage[T, NotUsed]], JavaList[IncomingMessageResult[T, NotUsed]], NotUsed] =
    Flow
      .fromFunction[JavaList[IncomingMessage[T, NotUsed]], Seq[IncomingMessage[T, NotUsed]]](_.asScala.toSeq)
      .via(
        ScalaSolrFlow
          .typed[T](collection, settings, i => binder.apply(i))(client)
      )
      .map(_.asJava)
      .asJava

  /**
   * Java API: creates a [[akka.stream.alpakka.solr.SolrFlowStage]] for [[SolrInputDocument]] from [[IncomingMessage]]
   * to lists of [[IncomingMessageResult]] with `passThrough` of type `C`.
   */
  def documentsWithPassThrough[C](
      collection: String,
      settings: SolrUpdateSettings,
      client: SolrClient
  ): javadsl.Flow[JavaList[IncomingMessage[SolrInputDocument, C]], JavaList[IncomingMessageResult[SolrInputDocument,
                                                                                                  C]], NotUsed] =
    Flow
      .fromFunction[JavaList[IncomingMessage[SolrInputDocument, C]], Seq[IncomingMessage[SolrInputDocument, C]]](
        _.asScala.toSeq
      )
      .via(
        ScalaSolrFlow
          .documentsWithPassThrough(collection, settings)(client)
      )
      .map(_.asJava)
      .asJava

  /**
   * Java API: creates a [[akka.stream.alpakka.solr.SolrFlowStage]] for type 'T' from [[IncomingMessage]]
   * to lists of [[IncomingMessageResult]] with `passThrough` of type `C`
   * and [[org.apache.solr.client.solrj.beans.DocumentObjectBinder]] for type 'T' .
   */
  def beansWithPassThrough[T, C](
      collection: String,
      settings: SolrUpdateSettings,
      client: SolrClient,
      clazz: Class[T]
  ): javadsl.Flow[JavaList[IncomingMessage[T, C]], JavaList[IncomingMessageResult[T, C]], NotUsed] =
    Flow
      .fromFunction[JavaList[IncomingMessage[T, C]], Seq[IncomingMessage[T, C]]](_.asScala.toSeq)
      .via(
        ScalaSolrFlow
          .beansWithPassThrough[T, C](collection, settings)(client)
      )
      .map(_.asJava)
      .asJava

  /**
   * Java API: creates a [[akka.stream.alpakka.solr.SolrFlowStage]] for type 'T' from [[IncomingMessage]]
   * to lists of [[IncomingMessageResult]] with `passThrough` of type `C` and `binder` of type `T`.
   */
  def typedWithPassThrough[T, C](
      collection: String,
      settings: SolrUpdateSettings,
      binder: Function[T, SolrInputDocument],
      client: SolrClient,
      clazz: Class[T]
  ): javadsl.Flow[JavaList[IncomingMessage[T, C]], JavaList[IncomingMessageResult[T, C]], NotUsed] =
    Flow
      .fromFunction[JavaList[IncomingMessage[T, C]], Seq[IncomingMessage[T, C]]](_.asScala.toSeq)
      .via(
        ScalaSolrFlow
          .typedWithPassThrough[T, C](collection, settings, i => binder.apply(i))(client)
      )
      .map(_.asJava)
      .asJava

}
