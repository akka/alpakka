/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.pravega.scaladsl

import akka.stream.scaladsl.{Flow, Keep, Sink, Source}
import akka.{Done, NotUsed}
import akka.stream.alpakka.pravega.impl.{PravegaTableSource, PravegaTableWriteFlow}
import akka.stream.alpakka.pravega.{TableSettings, TableWriterSettings}

import scala.concurrent.Future
import akka.stream.alpakka.pravega.impl.PravegaTableReadFlow

object PravegaTable {

  /**
   * Messages are read from a Pravega stream.
   *
   * Materialized value is a [[Future]] which completes to [[Done]] as soon as the Pravega reader is open.
   */
  def source[K, V](scope: String,
                   tableName: String,
                   keyFamily: String,
                   tableSettings: TableSettings[K, V]): Source[(K, V), Future[Done]] =
    Source.fromGraph(new PravegaTableSource(scope, tableName, keyFamily, tableSettings))

  /**
   * Keys, values and key families are extracted from incoming messages and written to Pravega table.
   * Messages are emitted downstream unchanged.
   */
  def writeFlow[A, K, V](scope: String, tableName: String, keyValueExtractor: A => (K, V))(
      implicit tableWriterSettings: TableWriterSettings[K, V]
  ): Flow[A, A, NotUsed] =
    Flow.fromGraph(new PravegaTableWriteFlow(scope, tableName, tableWriterSettings, keyValueExtractor, None))

  /**
   * Keys, values and key families are extracted from incoming messages and written to Pravega table.
   * Messages are emitted downstream unchanged.
   */
  def writeFlow[A, K, V](scope: String,
                         tableName: String,
                         keyValueExtractor: A => (K, V),
                         familyExtractor: A => String)(
      implicit tableWriterSettings: TableWriterSettings[K, V]
  ): Flow[A, A, NotUsed] =
    Flow.fromGraph(
      new PravegaTableWriteFlow(scope, tableName, tableWriterSettings, keyValueExtractor, familyExtractor)
    )

  /**
   * Incoming messages are written to a Pravega table KV.
   */
  def sink[A, K, V](scope: String, tableName: String, keyValueExtractor: A => (K, V))(
      implicit tableWriterSettings: TableWriterSettings[K, V]
  ): Sink[A, Future[Done]] =
    Flow[A].via(writeFlow(scope, tableName, keyValueExtractor)).toMat(Sink.ignore)(Keep.right)

  def sink[A, K, V](scope: String, tableName: String, keyValueExtractor: A => (K, V), familyExtractor: A => String)(
      implicit tableWriterSettings: TableWriterSettings[K, V]
  ): Sink[A, Future[Done]] =
    Flow[A].via(writeFlow(scope, tableName, keyValueExtractor, familyExtractor)).toMat(Sink.ignore)(Keep.right)

  /**
   * Keys, values and key families are extracted from incoming messages and written to Pravega table.
   * Messages are emitted downstream unchanged.
   */
  def readFlow[A, B, K, V](scope: String,
                           tableName: String,
                           keyExtractor: A => Option[K],
                           combine: (A, Option[V]) => B,
                           familyExtractor: Option[A => String])(
      implicit tableSettings: TableSettings[K, V]
  ): Flow[A, B, NotUsed] =
    Flow.fromGraph(new PravegaTableReadFlow(scope, tableName, tableSettings, keyExtractor, combine, familyExtractor))
}
