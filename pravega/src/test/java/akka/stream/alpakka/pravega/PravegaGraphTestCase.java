/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.pravega;

import akka.Done;
import akka.stream.alpakka.pravega.PravegaReaderGroupManager;
import akka.stream.javadsl.Source;

import com.typesafe.config.ConfigFactory;
import docs.javadsl.PravegaBaseTestCase;

import io.pravega.client.stream.ReaderGroup;
import io.pravega.client.stream.impl.JavaSerializer;
import org.junit.Assert;
import org.junit.Test;

import akka.japi.Pair;

import akka.stream.UniqueKillSwitch;
import akka.stream.KillSwitches;
import akka.stream.javadsl.Keep;
import akka.stream.javadsl.Sink;
import akka.stream.alpakka.pravega.javadsl.Pravega;

import java.util.Arrays;

import java.util.List;
import java.util.concurrent.*;

public class PravegaGraphTestCase extends PravegaBaseTestCase {

  private long timeoutSeconds = 10;

  @Test
  public void infiniteSourceTest()
      throws ExecutionException, InterruptedException, TimeoutException {

    String group = newGroup();
    String scope = newScope();
    String streamName = newStreamName();

    WriterSettings<String> writerSettings =
        WriterSettingsBuilder.<String>create(system).withSerializer(new JavaSerializer<>());

    WriterSettings<String> writerSettingsWithRoutingKey =
        WriterSettingsBuilder.<String>create(system)
            .withKeyExtractor((String str) -> str.substring(0, 1))
            .withSerializer(new JavaSerializer<>());

    ReaderSettings<String> readerSettings =
        ReaderSettingsBuilder.create(
                system
                    .settings()
                    .config()
                    .getConfig(ReaderSettingsBuilder.configPath())
                    .withFallback(ConfigFactory.parseString("group-name = " + group)))
            .withSerializer(new JavaSerializer<>());

    createStream(scope, streamName);

    final List<String> events = Arrays.asList("One", "Two", "Three");

    Sink<String, CompletionStage<Done>> sinkWithRouting =
        Pravega.sink(scope, streamName, writerSettings);

    CompletionStage<Done> doneWithRouting =
        Source.from(events).toMat(sinkWithRouting, Keep.right()).run(system);

    Sink<String, CompletionStage<Done>> sink =
        Pravega.sink(scope, streamName, writerSettingsWithRoutingKey);

    CompletionStage<Done> done = Source.from(events).toMat(sink, Keep.right()).run(system);

    CompletableFuture.allOf(done.toCompletableFuture(), doneWithRouting.toCompletableFuture())
        .get(timeoutSeconds, TimeUnit.SECONDS);

    CompletableFuture<Boolean> countTo200 = new CompletableFuture<>();

    ReaderGroup readerGroup;
    try (PravegaReaderGroupManager readerGroupManager =
        Pravega.readerGroup(scope, readerSettings.clientConfig())) {
      readerGroup = readerGroupManager.createReaderGroup(group, streamName);
    }

    Pair<UniqueKillSwitch, CompletionStage<Integer>> pair =
        Pravega.source(readerGroup, readerSettings)
            .map(e -> e.message())
            .viaMat(KillSwitches.single(), Keep.right())
            .toMat(
                Sink.fold(
                    events.size() * 2,
                    (acc, str) -> {
                      if (acc == 1) countTo200.complete(true);
                      return acc - 1;
                    }),
                Keep.both())
            .run(system);

    countTo200.get(timeoutSeconds, TimeUnit.SECONDS);

    LOGGER.info("Die, die by my hand.");
    pair.first().shutdown();

    Integer result = pair.second().toCompletableFuture().get(timeoutSeconds, TimeUnit.SECONDS);
    Assert.assertTrue("Read 6 events", result == 0);
  }
}
