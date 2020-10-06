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

public class Pravega {

  /**
   * Messages are read from a Pravega stream.
   *
   * <p>Materialized value is a future which completes to Done as soon as Pravega reader in open.
   */
  public static <V> Source<PravegaEvent<V>, CompletionStage<Done>> source(
      String scope, String streamName, ReaderSettings<V> readerSettings) {
    return Source.fromGraph(new PravegaSource<>(scope, streamName, readerSettings))
        .mapMaterializedValue(FutureConverters::<Done>toJava);
  }

  /** Incoming messages are written to Pravega stream and emitted unchanged. */
  public static <V> Flow<V, V, NotUsed> flow(
      String scope, String streamName, WriterSettings<V> writerSettings) {
    return Flow.fromGraph(new PravegaFlow<>(scope, streamName, writerSettings));
  }
  /** Incoming messages are written to Pravega. */
  public static <V> Sink<V, CompletionStage<Done>> sink(
      String scope, String streamName, WriterSettings<V> writerSettings) {
    return flow(scope, streamName, writerSettings).toMat(Sink.ignore(), Keep.right());
  }
  /**
   * Keys, values and key families are extracted from incoming messages and written to Pravega
   * table. Messages are emitted downstream unchanged.
   */
  public static <A, K, V> Flow<A, A, NotUsed> tableWriteFlow(
      String scope,
      String tableName,
      TableWriterSettings<K, V> tableWriterSettings,
      scala.Function1<A, scala.Tuple2<K, V>> keyValueExtractor,
      scala.Function1<A, String> familyExtractor) {
    return Flow.fromGraph(
        new PravegaTableWriteFlow<A, K, V>(
            scope, tableName, tableWriterSettings, keyValueExtractor, familyExtractor));
  }

  public static <A, K, V> Flow<A, A, NotUsed> tableWriteFlow(
      String scope,
      String tableName,
      TableWriterSettings<K, V> tableWriterSettings,
      scala.Function1<A, scala.Tuple2<K, V>> keyValueExtractor) {
    return Flow.fromGraph(
        new PravegaTableWriteFlow<A, K, V>(
            scope, tableName, tableWriterSettings, keyValueExtractor));
  }
  /** Incoming messages are written to a Pravega table KV. */
  public static <A, K, V> Sink<A, CompletionStage<Done>> tableSink(
      String scope,
      String tableName,
      TableWriterSettings<K, V> tableWriterSettings,
      scala.Function1<A, Pair<K, V>> keyValueExtractor,
      scala.Function1<A, String> familyExtractor) {
    return tableWriteFlow(
            scope,
            tableName,
            tableWriterSettings,
            keyValueExtractor.andThen(Pair::toScala),
            familyExtractor)
        .toMat(Sink.ignore(), Keep.right());
  }

  /** Incoming messages are written to a Pravega table KV. */
  public static <A, K, V> Sink<A, CompletionStage<Done>> tableSink(
      String scope,
      String tableName,
      TableWriterSettings<K, V> tableWriterSettings,
      scala.Function1<A, scala.Tuple2<K, V>> keyValueExtractor) {
    return tableWriteFlow(scope, tableName, tableWriterSettings, keyValueExtractor)
        .toMat(Sink.ignore(), Keep.right());
  }

  /**
   * Messages are read from a Pravega stream.
   *
   * <p>Materialized value is a [[Future]] which completes to [[Done]] as soon as the Pravega reader
   * is open.
   */
  public static <K, V> Source<Pair<K, V>, CompletionStage<Done>> tableSource(
      String scope, String tableName, String keyFamily, TableSettings<K, V> tableSettings) {
    return Source.fromGraph(
            new PravegaTableSource<K, V>(scope, tableName, keyFamily, tableSettings))
        .map(p -> new Pair<>(p._1, p._2))
        .mapMaterializedValue(FutureConverters::<Done>toJava);
  }
}
