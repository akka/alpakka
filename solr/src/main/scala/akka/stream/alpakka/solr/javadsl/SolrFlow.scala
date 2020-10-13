/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.solr.javadsl

import java.util.function.Function

import akka.NotUsed
import akka.stream.alpakka.solr.{scaladsl, SolrUpdateSettings, WriteMessage, WriteResult}
import akka.stream.javadsl
import akka.stream.scaladsl.Flow
import org.apache.solr.client.solrj.SolrClient
import org.apache.solr.common.SolrInputDocument

import scala.collection.JavaConverters._
import scala.collection.immutable

/**
 * Java API
 */
object SolrFlow {

  /**
   * Write `SolrInputDocument`s to Solr in a flow emitting `WriteResult`s containing the status.
   */
  def documents(
      collection: String,
      settings: SolrUpdateSettings,
      client: SolrClient
  ): javadsl.Flow[java.util.List[WriteMessage[SolrInputDocument, NotUsed]], java.util.List[
    WriteResult[SolrInputDocument, NotUsed]
  ], NotUsed] =
    Flow
      .fromFunction[java.util.List[WriteMessage[SolrInputDocument, NotUsed]],
                    immutable.Seq[WriteMessage[SolrInputDocument, NotUsed]]
      ](
        _.asScala.toIndexedSeq
      )
      .via(
        scaladsl.SolrFlow
          .documents(collection, settings)(client)
      )
      .map(_.asJava)
      .asJava

  /**
   * Write Java bean stream elements to Solr in a flow emitting `WriteResult`s containing the status.
   * The stream element classes must be annotated for use with [[org.apache.solr.client.solrj.beans.DocumentObjectBinder]] for conversion.
   */
  def beans[T](
      collection: String,
      settings: SolrUpdateSettings,
      client: SolrClient,
      clazz: Class[T]
  ): javadsl.Flow[java.util.List[WriteMessage[T, NotUsed]], java.util.List[WriteResult[T, NotUsed]], NotUsed] =
    Flow
      .fromFunction[java.util.List[WriteMessage[T, NotUsed]], immutable.Seq[WriteMessage[T, NotUsed]]](
        _.asScala.toIndexedSeq
      )
      .via(
        scaladsl.SolrFlow
          .beans[T](collection, settings)(client)
      )
      .map(_.asJava)
      .asJava

  /**
   * Write stream elements to Solr in a flow emitting `WriteResult`s containing the status.
   *
   * @param binder a conversion function to create `SolrInputDocument`s of the stream elements
   */
  def typeds[T](
      collection: String,
      settings: SolrUpdateSettings,
      binder: Function[T, SolrInputDocument],
      client: SolrClient,
      clazz: Class[T]
  ): javadsl.Flow[java.util.List[WriteMessage[T, NotUsed]], java.util.List[WriteResult[T, NotUsed]], NotUsed] =
    Flow
      .fromFunction[java.util.List[WriteMessage[T, NotUsed]], immutable.Seq[WriteMessage[T, NotUsed]]](
        _.asScala.toIndexedSeq
      )
      .via(
        scaladsl.SolrFlow
          .typeds[T](collection, settings, i => binder.apply(i))(client)
      )
      .map(_.asJava)
      .asJava

  /**
   * Write `SolrInputDocument`s to Solr in a flow emitting `WriteResult`s containing the status.
   *
   * @tparam PT pass-through type
   */
  def documentsWithPassThrough[PT](
      collection: String,
      settings: SolrUpdateSettings,
      client: SolrClient
  ): javadsl.Flow[java.util.List[WriteMessage[SolrInputDocument, PT]], java.util.List[WriteResult[SolrInputDocument,
                                                                                                  PT
  ]], NotUsed] =
    Flow
      .fromFunction[java.util.List[WriteMessage[SolrInputDocument, PT]], immutable.Seq[WriteMessage[SolrInputDocument,
                                                                                                    PT
      ]]](
        _.asScala.toIndexedSeq
      )
      .via(
        scaladsl.SolrFlow
          .documentsWithPassThrough(collection, settings)(client)
      )
      .map(_.asJava)
      .asJava

  /**
   * Write Java bean stream elements to Solr in a flow emitting `WriteResult`s containing the status.
   * The stream element classes must be annotated for use with [[org.apache.solr.client.solrj.beans.DocumentObjectBinder]] for conversion.
   *
   * @tparam PT pass-through type
   */
  def beansWithPassThrough[T, PT](
      collection: String,
      settings: SolrUpdateSettings,
      client: SolrClient,
      clazz: Class[T]
  ): javadsl.Flow[java.util.List[WriteMessage[T, PT]], java.util.List[WriteResult[T, PT]], NotUsed] =
    Flow
      .fromFunction[java.util.List[WriteMessage[T, PT]], immutable.Seq[WriteMessage[T, PT]]](_.asScala.toIndexedSeq)
      .via(
        scaladsl.SolrFlow
          .beansWithPassThrough[T, PT](collection, settings)(client)
      )
      .map(_.asJava)
      .asJava

  /**
   * Write stream elements to Solr in a flow emitting `WriteResult`s containing the status.
   *
   * @param binder a conversion function to create `SolrInputDocument`s of the stream elements
   * @tparam PT pass-through type
   */
  def typedsWithPassThrough[T, PT](
      collection: String,
      settings: SolrUpdateSettings,
      binder: Function[T, SolrInputDocument],
      client: SolrClient,
      clazz: Class[T]
  ): javadsl.Flow[java.util.List[WriteMessage[T, PT]], java.util.List[WriteResult[T, PT]], NotUsed] =
    Flow
      .fromFunction[java.util.List[WriteMessage[T, PT]], immutable.Seq[WriteMessage[T, PT]]](_.asScala.toIndexedSeq)
      .via(
        scaladsl.SolrFlow
          .typedsWithPassThrough[T, PT](collection, settings, i => binder.apply(i))(client)
      )
      .map(_.asJava)
      .asJava

}
