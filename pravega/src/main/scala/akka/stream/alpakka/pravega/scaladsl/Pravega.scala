/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.pravega.scaladsl
import akka.stream.scaladsl.{Flow, Keep, Sink, Source}
import akka.{Done, NotUsed}
import akka.stream.alpakka.pravega.impl.{PravegaFlow, PravegaSource, PravegaTableSource, PravegaTableWriteFlow}
import akka.stream.alpakka.pravega.{PravegaEvent, ReaderSettings, TableSettings, TableWriterSettings, WriterSettings}

import scala.concurrent.Future
import akka.stream.alpakka.pravega.impl.PravegaTableReadFlow

object Pravega {

  /**
   * Messages are read from a Pravega stream.
   *
   * Materialized value is a [[Future]] which completes to [[Done]] as soon as the Pravega reader is open.
   */
  def source[A](scope: String,
                streamName: String)(implicit readerSettings: ReaderSettings[A]): Source[PravegaEvent[A], Future[Done]] =
    Source.fromGraph(new PravegaSource(scope, streamName, readerSettings))

  /**
   * Incoming messages are written to Pravega stream and emitted unchanged.
   */
  def flow[A](scope: String, streamName: String)(
      implicit writerSettings: WriterSettings[A]
  ): Flow[A, A, NotUsed] =
    Flow.fromGraph(new PravegaFlow(scope, streamName, writerSettings))

  /**
   * Incoming messages are written to Pravega.
   */
  def sink[A](scope: String, streamName: String)(
      implicit writerSettings: WriterSettings[A]
  ): Sink[A, Future[Done]] =
    Flow[A].via(flow(scope, streamName)).toMat(Sink.ignore)(Keep.right)

  /**
   * Messages are read from a Pravega stream.
   *
   * Materialized value is a [[Future]] which completes to [[Done]] as soon as the Pravega reader is open.
   */
  def tableSource[K, V](scope: String,
                        tableName: String,
                        keyFamily: String,
                        tableSettings: TableSettings[K, V]): Source[(K, V), Future[Done]] =
    Source.fromGraph(new PravegaTableSource(scope, tableName, keyFamily, tableSettings))

  /**
   * Keys, values and key families are extracted from incoming messages and written to Pravega table.
   * Messages are emitted downstream unchanged.
   */
  def tableWriteFlow[A, K, V](scope: String, tableName: String, keyValueExtractor: A => (K, V))(
      implicit tableWriterSettings: TableWriterSettings[K, V]
  ): Flow[A, A, NotUsed] =
    Flow.fromGraph(new PravegaTableWriteFlow(scope, tableName, tableWriterSettings, keyValueExtractor, None))

  /**
   * Keys, values and key families are extracted from incoming messages and written to Pravega table.
   * Messages are emitted downstream unchanged.
   */
  def tableWriteFlow[A, K, V](scope: String,
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
  def tableSink[A, K, V](scope: String, tableName: String, keyValueExtractor: A => (K, V))(
      implicit tableWriterSettings: TableWriterSettings[K, V]
  ): Sink[A, Future[Done]] =
    Flow[A].via(tableWriteFlow(scope, tableName, keyValueExtractor)).toMat(Sink.ignore)(Keep.right)

  def tableSink[A, K, V](scope: String,
                         tableName: String,
                         keyValueExtractor: A => (K, V),
                         familyExtractor: A => String)(
      implicit tableWriterSettings: TableWriterSettings[K, V]
  ): Sink[A, Future[Done]] =
    Flow[A].via(tableWriteFlow(scope, tableName, keyValueExtractor, familyExtractor)).toMat(Sink.ignore)(Keep.right)

  /**
   * Keys, values and key families are extracted from incoming messages and written to Pravega table.
   * Messages are emitted downstream unchanged.
   */
  def tableReadFlow[A, B, K, V](scope: String,
                                tableName: String,
                                keyExtractor: A => Option[K],
                                combine: (A, Option[V]) => B,
                                familyExtractor: Option[A => String])(
      implicit tableSettings: TableSettings[K, V]
  ): Flow[A, B, NotUsed] =
    Flow.fromGraph(new PravegaTableReadFlow(scope, tableName, tableSettings, keyExtractor, combine, familyExtractor))
}
