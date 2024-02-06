/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.pravega.scaladsl

import akka.annotation.ApiMayChange
import akka.stream.scaladsl.{Flow, Keep, Sink, Source}
import akka.{Done, NotUsed}
import akka.stream.alpakka.pravega.impl.{PravegaTableSource, PravegaTableWriteFlow}
import akka.stream.alpakka.pravega.{TableReaderSettings, TableSettings, TableWriterSettings}

import scala.concurrent.Future
import akka.stream.alpakka.pravega.impl.PravegaTableReadFlow
import akka.stream.alpakka.pravega.TableEntry

@ApiMayChange
object PravegaTable {

  /**
   * Messages are read from a Pravega stream.
   *
   * Materialized value is a [[scala.concurrent.Future]] which completes to [[Done]] as soon as the Pravega reader is open.
   */
  def source[K, V](scope: String,
                   tableName: String,
                   tableReaderSettings: TableReaderSettings[K, V]
  ): Source[TableEntry[V], Future[Done]] =
    Source.fromGraph(
      new PravegaTableSource[K, V](
        scope,
        tableName,
        tableReaderSettings
      )
    )

  /**
   * A flow from key to and Option[value].
   */
  def readFlow[K, V](scope: String,
                     tableName: String,
                     tableSettings: TableSettings[K, V]
  ): Flow[K, Option[V], NotUsed] =
    Flow.fromGraph(new PravegaTableReadFlow(scope, tableName, tableSettings))

  /**
   * Keys and values are extracted from incoming messages and written to Pravega table.
   * Messages are emitted downstream unchanged.
   */
  def writeFlow[K, V](scope: String,
                      tableName: String,
                      tableWriterSettings: TableWriterSettings[K, V]
  ): Flow[(K, V), (K, V), NotUsed] =
    Flow.fromGraph(
      new PravegaTableWriteFlow[(K, V), K, V]((o: (K, V)) => o, scope, tableName, tableWriterSettings)
    )

  /**
   * Incoming messages are written to a Pravega table KV.
   */
  def sink[K, V](scope: String,
                 tableName: String,
                 tableWriterSettings: TableWriterSettings[K, V]
  ): Sink[(K, V), Future[Done]] =
    Flow[(K, V)].via(writeFlow(scope, tableName, tableWriterSettings)).toMat(Sink.ignore)(Keep.right)

}
