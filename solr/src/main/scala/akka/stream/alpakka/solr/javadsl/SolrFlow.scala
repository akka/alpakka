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
import org.apache.solr.client.solrj.SolrClient
import org.apache.solr.common.SolrInputDocument

import scala.collection.JavaConverters._

object SolrFlow {

  /**
   * Java API: creates a [[akka.stream.alpakka.solr.SolrFlowStage]] for [[SolrInputDocument]]
   * from [[IncomingMessage]] to sequences of [[IncomingMessageResult]].
   */
  def document(
      collection: String,
      settings: SolrUpdateSettings,
      client: SolrClient
  ): javadsl.Flow[IncomingMessage[SolrInputDocument, NotUsed], JavaList[IncomingMessageResult[SolrInputDocument,
                                                                                              NotUsed]], NotUsed] =
    ScalaSolrFlow
      .document(collection, settings)(client)
      .map(_.asJava)
      .asJava

  /**
   * Java API: creates a [[akka.stream.alpakka.solr.SolrFlowStage]] for type `T` from [[IncomingMessage]] to sequences
   * of [[IncomingMessageResult]] with [[org.apache.solr.client.solrj.beans.DocumentObjectBinder]].
   */
  def bean[T](
      collection: String,
      settings: SolrUpdateSettings,
      client: SolrClient,
      clazz: Class[T]
  ): javadsl.Flow[IncomingMessage[T, NotUsed], JavaList[IncomingMessageResult[T, NotUsed]], NotUsed] =
    ScalaSolrFlow
      .bean[T](collection, settings)(client)
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
  ): javadsl.Flow[IncomingMessage[T, NotUsed], JavaList[IncomingMessageResult[T, NotUsed]], NotUsed] =
    ScalaSolrFlow
      .typed[T](collection, settings, i => binder.apply(i))(client)
      .map(_.asJava)
      .asJava

  /**
   * Java API: creates a [[akka.stream.alpakka.solr.SolrFlowStage]] for [[SolrInputDocument]] from [[IncomingMessage]]
   * to lists of [[IncomingMessageResult]] with `passThrough` of type `C`.
   */
  def documentWithPassThrough[C](
      collection: String,
      settings: SolrUpdateSettings,
      client: SolrClient
  ): javadsl.Flow[IncomingMessage[SolrInputDocument, C], JavaList[IncomingMessageResult[SolrInputDocument, C]], NotUsed] =
    ScalaSolrFlow
      .documentWithPassThrough[C](collection, settings)(client)
      .map(_.asJava)
      .asJava

  /**
   * Java API: creates a [[akka.stream.alpakka.solr.SolrFlowStage]] for type 'T' from [[IncomingMessage]]
   * to lists of [[IncomingMessageResult]] with `passThrough` of type `C`
   * and [[org.apache.solr.client.solrj.beans.DocumentObjectBinder]] for type 'T' .
   */
  def beanWithPassThrough[T, C](
      collection: String,
      settings: SolrUpdateSettings,
      client: SolrClient,
      clazz: Class[T]
  ): javadsl.Flow[IncomingMessage[T, C], JavaList[IncomingMessageResult[T, C]], NotUsed] =
    ScalaSolrFlow
      .beanWithPassThrough[T, C](collection, settings)(client)
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
  ): javadsl.Flow[IncomingMessage[T, C], JavaList[IncomingMessageResult[T, C]], NotUsed] =
    ScalaSolrFlow
      .typedWithPassThrough[T, C](collection, settings, i => binder.apply(i))(client)
      .map(_.asJava)
      .asJava

}
