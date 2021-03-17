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
import akka.stream.alpakka.pravega.impl.PravegaFlow;
import akka.stream.alpakka.pravega.impl.PravegaSource;

public class PravegaTable {

  /**
   * Keys, values and key families are extracted from incoming messages and written to Pravega
   * table. Messages are emitted downstream unchanged.
   */
  public static <A, K, V> Flow<A, A, NotUsed> writeFlow(
      String scope,
      String tableName,
      TableWriterSettings<K, V> tableWriterSettings,
      scala.Function1<A, Pair<K, V>> keyValueExtractor,
      scala.Function1<A, String> familyExtractor) {
    return Flow.fromGraph(
        new PravegaTableWriteFlow<A, K, V>(
            scope,
            tableName,
            tableWriterSettings,
            keyValueExtractor.andThen(Pair::toScala),
            familyExtractor));
  }

  public static <A, K, V> Flow<A, A, NotUsed> writeFlow(
      String scope,
      String tableName,
      TableWriterSettings<K, V> tableWriterSettings,
      scala.Function1<A, Pair<K, V>> keyValueExtractor) {
    return Flow.fromGraph(
        new PravegaTableWriteFlow<A, K, V>(
            scope, tableName, tableWriterSettings, keyValueExtractor.andThen(Pair::toScala)));
  }
  /** Incoming messages are written to a Pravega table KV. */
  public static <A, K, V> Sink<A, CompletionStage<Done>> sink(
      String scope,
      String tableName,
      TableWriterSettings<K, V> tableWriterSettings,
      scala.Function1<A, Pair<K, V>> keyValueExtractor,
      scala.Function1<A, String> familyExtractor) {
    return writeFlow(scope, tableName, tableWriterSettings, keyValueExtractor, familyExtractor)
        .toMat(Sink.ignore(), Keep.right());
  }

  /** Incoming messages are written to a Pravega table KV. */
  public static <A, K, V> Sink<A, CompletionStage<Done>> sink(
      String scope,
      String tableName,
      TableWriterSettings<K, V> tableWriterSettings,
      scala.Function1<A, Pair<K, V>> keyValueExtractor) {
    return writeFlow(scope, tableName, tableWriterSettings, keyValueExtractor)
        .toMat(Sink.ignore(), Keep.right());
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
            new PravegaTableSource<K, V>(scope, tableName, keyFamily, tableSettings))
        .map(p -> new Pair<>(p._1, p._2))
        .mapMaterializedValue(FutureConverters::<Done>toJava);
  }
}
