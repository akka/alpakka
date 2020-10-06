/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.pravega.javadsl;

import akka.Done;
import akka.NotUsed;
import akka.japi.Pair;
import akka.stream.alpakka.pravega.*;
import akka.stream.alpakka.pravega.impl.PravegaTableSource;
import akka.stream.alpakka.pravega.impl.PravegaTableWriteFlow;
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.Keep;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;

import scala.compat.java8.FutureConverters;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;

import io.pravega.client.tables.TableEntry;

import scala.compat.java8.functionConverterImpls.FromJavaFunction;

public class PravegaTable {

  private static <K, V> Pair<K, V> toPair(TableEntry<K, V> entry) {
    return new Pair<>(entry.getKey().getKey(), entry.getValue());
  }

  /**
   * Keys, values and key families are extracted from incoming messages and written to Pravega
   * table. Messages are emitted downstream unchanged.
   */
  public static <K, V> Flow<Pair<K, V>, Pair<K, V>, NotUsed> writeFlow(
      String scope,
      String tableName,
      TableWriterSettings<K, V> tableWriterSettings,
      Function<K, String> familyExtractor) {
    return Flow.fromGraph(
        new PravegaTableWriteFlow<Pair<K, V>, K, V>(
            Pair::toScala,
            scope,
            tableName,
            tableWriterSettings,
            new FromJavaFunction<>(familyExtractor)));
  }

  public static <K, V> Flow<Pair<K, V>, Pair<K, V>, NotUsed> writeFlow(
      String scope, String tableName, TableWriterSettings<K, V> tableWriterSettings) {
    return Flow.fromGraph(
        new PravegaTableWriteFlow<Pair<K, V>, K, V>(
            Pair::toScala, scope, tableName, tableWriterSettings));
  }
  /** Incoming messages are written to a Pravega table KV. */
  public static <K, V> Sink<Pair<K, V>, CompletionStage<Done>> sink(
      String scope,
      String tableName,
      TableWriterSettings<K, V> tableWriterSettings,
      Function<K, String> familyExtractor) {
    return writeFlow(scope, tableName, tableWriterSettings, familyExtractor)
        .toMat(Sink.ignore(), Keep.right());
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
  public static <K, V> Source<Pair<K, V>, CompletionStage<Done>> source(
      String scope, String tableName, String keyFamily, TableSettings<K, V> tableSettings) {
    return Source.fromGraph(
            new PravegaTableSource<>(
                PravegaTable::toPair, scope, tableName, keyFamily, tableSettings))
        .mapMaterializedValue(FutureConverters::toJava);
  }
}
