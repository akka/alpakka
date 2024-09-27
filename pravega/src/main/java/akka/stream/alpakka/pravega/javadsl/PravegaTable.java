/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.pravega.javadsl;

import akka.Done;
import akka.NotUsed;
import akka.annotation.ApiMayChange;
import akka.japi.Pair;
import akka.stream.alpakka.pravega.*;
import akka.stream.alpakka.pravega.impl.PravegaTableSource;
import akka.stream.alpakka.pravega.impl.PravegaTableWriteFlow;
import akka.stream.alpakka.pravega.impl.PravegaTableReadFlow;
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.Keep;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;

import java.util.Optional;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;
import java.nio.ByteBuffer;

import io.pravega.client.tables.TableKey;

import scala.jdk.javaapi.FutureConverters;
import scala.jdk.javaapi.FunctionConverters;
import scala.jdk.javaapi.OptionConverters;

import scala.Option;

@ApiMayChange
public class PravegaTable {

  /**
   * Keys, values and key families are extracted from incoming messages and written to Pravega
   * table. Messages are emitted downstream unchanged.
   */
  public static <K, V> Flow<Pair<K, V>, Pair<K, V>, NotUsed> writeFlow(
      String scope, String tableName, TableWriterSettings<K, V> tableWriterSettings) {
    return Flow.fromGraph(
        new PravegaTableWriteFlow<Pair<K, V>, K, V>(
            Pair::toScala, scope, tableName, tableWriterSettings));
  }

  /** Incoming messages are written to a Pravega table KV. */
  public static <K, V> Sink<Pair<K, V>, CompletionStage<Done>> sink(
      String scope, String tableName, TableWriterSettings<K, V> tableWriterSettings) {
    return writeFlow(scope, tableName, tableWriterSettings).toMat(Sink.ignore(), Keep.right());
  }

  /**
   * Messages are read from a Pravega table.
   *
   * <p>Materialized value is a [[Future]] which completes to [[Done]] as soon as the Pravega reader
   * is open.
   */
  public static <K, V> Source<TableEntry<V>, CompletionStage<Done>> source(
      String scope, String tableName, TableReaderSettings<K, V> tableReaderSettings) {
    return Source.fromGraph(new PravegaTableSource<K, V>(scope, tableName, tableReaderSettings))
        .mapMaterializedValue(FutureConverters::asJava);
  }
  /** A flow from key to and Option[value]. */
  public static <K, V> Flow<K, Optional<V>, NotUsed> readFlow(
      String scope, String tableName, TableSettings<K, V> tableSettings) {
    return Flow.fromGraph(new PravegaTableReadFlow<K, V>(scope, tableName, tableSettings))
        .map(OptionConverters::toJava);
  }
}
