/*
 * Copyright (C) since 2016 Lightbend Inc. <https://akka.io>
 */

package docs.javadsl;

import static org.hamcrest.CoreMatchers.*;
import static org.hamcrest.MatcherAssert.assertThat;

import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.stream.alpakka.recordio.javadsl.RecordIOFraming;
import akka.stream.alpakka.testkit.javadsl.LogCapturingJunit4;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import akka.testkit.javadsl.TestKit;
import akka.util.ByteString;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.junit.AfterClass;
import org.junit.Rule;
import org.junit.Test;

public class RecordIOFramingTest {
  @Rule public final LogCapturingJunit4 logCapturing = new LogCapturingJunit4();

  private static final ActorSystem system = ActorSystem.create();

  @AfterClass
  public static void afterAll() {
    TestKit.shutdownActorSystem(system);
  }

  @Test
  public void parseStream() throws InterruptedException, ExecutionException, TimeoutException {
    // #run-via-scanner
    String firstRecordData =
        "{\"type\": \"SUBSCRIBED\",\"subscribed\": {\"framework_id\": {\"value\":\"12220-3440-12532-2345\"},\"heartbeat_interval_seconds\":15.0}";
    String secondRecordData = "{\"type\":\"HEARTBEAT\"}";

    String firstRecordWithPrefix = "121\n" + firstRecordData;
    String secondRecordWithPrefix = "20\n" + secondRecordData;

    Source<ByteString, NotUsed> basicSource =
        Source.single(ByteString.fromString(firstRecordWithPrefix + secondRecordWithPrefix));

    CompletionStage<List<ByteString>> result =
        basicSource.via(RecordIOFraming.scanner()).runWith(Sink.seq(), system);
    // #run-via-scanner

    // #result
    List<ByteString> byteStrings = result.toCompletableFuture().get(1, TimeUnit.SECONDS);

    assertThat(byteStrings.get(0), is(ByteString.fromString(firstRecordData)));
    assertThat(byteStrings.get(1), is(ByteString.fromString(secondRecordData)));
    // #result
  }
}
