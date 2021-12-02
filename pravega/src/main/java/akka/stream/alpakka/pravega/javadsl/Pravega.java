/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.pravega.javadsl;

import akka.Done;
import akka.NotUsed;
import akka.annotation.ApiMayChange;
import akka.stream.alpakka.pravega.PravegaEvent;

import akka.stream.alpakka.pravega.WriterSettings;
import akka.stream.alpakka.pravega.PravegaReaderGroupManager;
import akka.stream.alpakka.pravega.*;
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.Keep;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;

import io.pravega.client.ClientConfig;
import io.pravega.client.stream.ReaderGroup;
import scala.compat.java8.FutureConverters;
import java.util.concurrent.CompletionStage;

import akka.stream.alpakka.pravega.impl.PravegaFlow;
import akka.stream.alpakka.pravega.impl.PravegaSource;

@ApiMayChange
public class Pravega {

  public static PravegaReaderGroupManager readerGroup(String scope, ClientConfig clientConfig) {

    return new PravegaReaderGroupManager(scope, clientConfig);
  }
  /**
   * Messages are read from a Pravega stream.
   *
   * <p>Materialized value is a future which completes to Done as soon as Pravega reader in open.
   */
  public static <V> Source<PravegaEvent<V>, CompletionStage<Done>> source(
      ReaderGroup readerGroup, ReaderSettings<V> readerSettings) {
    return Source.fromGraph(new PravegaSource<>(readerGroup, readerSettings))
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
}
