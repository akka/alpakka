/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.pravega;

import akka.Done;
import akka.japi.function.Function2;
import akka.stream.javadsl.Source;

import docs.javadsl.PravegaBaseTestCase;
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

import java.util.concurrent.*;

public class PravegaGraphTestCase extends PravegaBaseTestCase {

  private WriterSettings<String> writerSettings =
      WriterSettingsBuilder.<String>create(system).withSerializer(new JavaSerializer<>());

  private WriterSettings<String> writerSettingsWithRoutingKey =
      WriterSettingsBuilder.<String>create(system)
          .withKeyExtractor((String str) -> str.substring(0, 1))
          .withSerializer(new JavaSerializer<>());

  private ReaderSettings<String> readerSettings =
      ReaderSettingsBuilder.create(system).withSerializer(new JavaSerializer<>());

  @Test
  public void infiniteSourceTest()
      throws ExecutionException, InterruptedException, TimeoutException {

    Sink<String, CompletionStage<Done>> sinkWithRouting =
        Pravega.sink(scope, streamName, writerSettings);

    CompletionStage<Done> doneWithRouting =
        Source.from(Arrays.asList("One", "Two", "Three"))
            .toMat(sinkWithRouting, Keep.right())
            .run(materializer);

    Sink<String, CompletionStage<Done>> sink =
        Pravega.sink(scope, streamName, writerSettingsWithRoutingKey);

    CompletionStage<Done> done =
        Source.from(Arrays.asList("One", "Two", "Three"))
            .toMat(sink, Keep.right())
            .run(materializer);

    CompletableFuture.allOf(done.toCompletableFuture(), doneWithRouting.toCompletableFuture())
        .get();

    CompletableFuture<Boolean> countTo200 = new CompletableFuture<>();
    // #reading
    Pair<UniqueKillSwitch, CompletionStage<Integer>> pair =
        Pravega.source(scope, streamName, readerSettings)
            .map(e -> e.message())
            .viaMat(KillSwitches.single(), Keep.right())
            .toMat(
                Sink.fold(
                    0,
                    new Function2<Integer, String, Integer>() {
                      @Override
                      public Integer apply(Integer acc, String str) throws Exception, Exception {
                        if (acc == 5) countTo200.complete(true);
                        return acc + 1;
                      }
                    }),
                Keep.both())
            .run(materializer);
    // #reading

    countTo200.get(10, TimeUnit.SECONDS);

    LOGGER.info("Die, die by my hand.");
    pair.first().shutdown();

    Integer result = pair.second().toCompletableFuture().get();
    Assert.assertTrue("Read 6 events", result == 6);
  }
}
