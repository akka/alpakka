/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.pravega.scaladsl

import akka.annotation.ApiMayChange
import akka.stream.scaladsl.{Flow, Keep, Sink, Source}
import akka.{Done, NotUsed}
import akka.stream.alpakka.pravega.impl.{PravegaTableSource, PravegaTableWriteFlow}
import akka.stream.alpakka.pravega.{TableReaderSettings, TableSettings, TableWriterSettings}

import scala.concurrent.Future
import akka.stream.alpakka.pravega.impl.PravegaTableReadFlow

@ApiMayChange
object PravegaTable {

  /**
   * Messages are read from a Pravega stream.
   *
   * Materialized value is a [[Future]] which completes to [[Done]] as soon as the Pravega reader is open.
   */
  def source[K, V](scope: String,
                   tableName: String,
                   keyFamily: String,
                   tableReaderSettings: TableReaderSettings[K, V]): Source[(K, V), Future[Done]] =
    Source.fromGraph(
      new PravegaTableSource[Tuple2[K, V], K, V](te => (te.getKey.getKey, te.getValue),
                                                 scope,
                                                 tableName,
                                                 keyFamily,
                                                 tableReaderSettings)
    )

  /**
   * Keys, values and key families are extracted from incoming messages and written to Pravega table.
   * Messages are emitted downstream unchanged.
   */
  def writeFlow[K, V](scope: String, tableName: String)(
      implicit tableWriterSettings: TableWriterSettings[K, V]
  ): Flow[(K, V), (K, V), NotUsed] =
    Flow.fromGraph(
      new PravegaTableWriteFlow[(K, V), K, V]((o: (K, V)) => o, scope, tableName, tableWriterSettings, None)
    )

  /**
   * Keys, values and key families are extracted from incoming messages and written to Pravega table.
   * Messages are emitted downstream unchanged.
   */
  def writeFlow[K, V](scope: String, tableName: String, familyExtractor: K => String)(
      implicit tableWriterSettings: TableWriterSettings[K, V]
  ): Flow[(K, V), (K, V), NotUsed] =
    Flow.fromGraph(
      new PravegaTableWriteFlow[(K, V), K, V]((o: (K, V)) => o,
                                              scope,
                                              tableName,
                                              tableWriterSettings,
                                              Some(familyExtractor))
    )

  /**
   * Incoming messages are written to a Pravega table KV.
   */
  def sink[K, V](scope: String, tableName: String)(
      implicit tableWriterSettings: TableWriterSettings[K, V]
  ): Sink[(K, V), Future[Done]] =
    Flow[(K, V)].via(writeFlow(scope, tableName)).toMat(Sink.ignore)(Keep.right)

  def sink[K, V](scope: String, tableName: String, familyExtractor: K => String)(
      implicit tableWriterSettings: TableWriterSettings[K, V]
  ): Sink[(K, V), Future[Done]] =
    Flow[(K, V)].via(writeFlow(scope, tableName, familyExtractor)).toMat(Sink.ignore)(Keep.right)

  /**
   * Keys, values and key families are extracted from incoming messages and written to Pravega table.
   * Messages are emitted downstream unchanged.
   */
  def readFlow[K, V](scope: String, tableName: String, familyExtractor: K => String)(
      implicit tableSettings: TableSettings[K, V]
  ): Flow[K, Option[V], NotUsed] =
    Flow.fromGraph(new PravegaTableReadFlow(scope, tableName, tableSettings, familyExtractor))
}
