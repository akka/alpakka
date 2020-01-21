/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package docs.javadsl;

import akka.actor.ActorSystem;
import akka.stream.ActorMaterializer;
import akka.stream.Materializer;
import akka.stream.alpakka.json.javadsl.JsonReader;
import akka.stream.alpakka.testkit.javadsl.LogCapturingJunit4;
import akka.stream.javadsl.*;
import akka.testkit.javadsl.TestKit;
import akka.util.ByteString;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.hamcrest.CoreMatchers.hasItems;
import static org.junit.Assert.assertThat;

public class JsonReaderUsageTest {
  @Rule public final LogCapturingJunit4 logCapturing = new LogCapturingJunit4();

  private static ActorSystem system;
  private static Materializer materializer;

  @Test
  public void jsonParser() throws InterruptedException, ExecutionException, TimeoutException {
    final String firstDoc = "{\"name\":\"test1\"}";
    final String secondDoc = "{\"name\":\"test2\"}";
    final String thirdDoc = "{\"name\":\"test3\"}";

    final ByteString doc =
        ByteString.fromString(
            "{"
                + "\"size\": 3,"
                + "\"rows\": ["
                + "{\"id\": 1, \"doc\":"
                + firstDoc
                + "},"
                + "{\"id\": 2, \"doc\":"
                + secondDoc
                + "},"
                + "{\"id\": 3, \"doc\":"
                + thirdDoc
                + "}"
                + "]}");

    // #usage
    final CompletionStage<List<ByteString>> resultStage =
        Source.single(doc)
            .via(JsonReader.select("$.rows[*].doc"))
            .runWith(Sink.seq(), materializer);
    // #usage

    resultStage
        .thenAccept(
            (list) -> {
              assertThat(
                  list,
                  hasItems(
                      ByteString.fromString(firstDoc),
                      ByteString.fromString(secondDoc),
                      ByteString.fromString(thirdDoc)));
            })
        .toCompletableFuture()
        .get(5, TimeUnit.SECONDS);
  }

  @BeforeClass
  public static void setup() throws Exception {
    system = ActorSystem.create();
    materializer = ActorMaterializer.create(system);
  }

  @AfterClass
  public static void teardown() throws Exception {
    TestKit.shutdownActorSystem(system);
  }
}
