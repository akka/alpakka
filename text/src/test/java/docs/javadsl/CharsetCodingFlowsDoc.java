/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.javadsl;

import akka.actor.ActorSystem;
import akka.stream.ActorMaterializer;
import akka.stream.Materializer;

// #encoding
import akka.stream.alpakka.testkit.javadsl.LogCapturingJunit4;
import akka.stream.alpakka.text.javadsl.TextFlow;
import akka.stream.IOResult;
import akka.stream.javadsl.FileIO;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import akka.util.ByteString;

import java.nio.charset.StandardCharsets;

// #encoding
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class CharsetCodingFlowsDoc {
  @Rule public final LogCapturingJunit4 logCapturing = new LogCapturingJunit4();

  private static final ActorSystem system = ActorSystem.create();
  private static final Materializer materializer = ActorMaterializer.create(system);

  @AfterClass
  public static void afterAll() {
    system.terminate();
  }

  @Test
  public void encodingExample() throws Exception {
    Path targetFile = Paths.get("target/outdata.txt");

    Properties properties = System.getProperties();
    List<String> strings =
        properties.stringPropertyNames().stream()
            .map(p -> p + " -> " + properties.getProperty(p))
            .collect(Collectors.toList());
    // #encoding
    Source<String, ?> stringSource = // ...
        // #encoding
        Source.from(strings);
    final CompletionStage<IOResult> streamCompletion =
        // #encoding
        stringSource
            .via(TextFlow.encoding(StandardCharsets.US_ASCII))
            .intersperse(ByteString.fromString("\n"))
            .runWith(FileIO.toPath(targetFile), materializer);
    // #encoding
    IOResult result = streamCompletion.toCompletableFuture().get(1, TimeUnit.SECONDS);
    assertTrue(result.wasSuccessful());
  }

  @Test
  public void decodingExample() throws Exception {
    ByteString utf16bytes = ByteString.fromString("äåûßêëé", StandardCharsets.UTF_16);
    // #decoding
    Source<ByteString, ?> byteStringSource = // ...
        // #decoding
        Source.single(utf16bytes);
    CompletionStage<List<String>> streamCompletion =
        // #decoding
        byteStringSource
            .via(TextFlow.decoding(StandardCharsets.UTF_16))
            .runWith(Sink.seq(), materializer);
    // #decoding
    List<String> result = streamCompletion.toCompletableFuture().get(1, TimeUnit.SECONDS);
    assertEquals(Arrays.asList("äåûßêëé"), result);
  }

  @Test
  public void transcodingExample() throws Exception {
    Path targetFile = Paths.get("target/outdata.txt");
    ByteString utf16bytes = ByteString.fromString("äåûßêëé", StandardCharsets.UTF_16);

    // #transcoding
    Source<ByteString, ?> byteStringSource = // ...
        // #transcoding
        Source.single(utf16bytes);
    CompletionStage<IOResult> streamCompletion =
        // #transcoding
        byteStringSource
            .via(TextFlow.transcoding(StandardCharsets.UTF_16, StandardCharsets.UTF_8))
            .runWith(FileIO.toPath(targetFile), materializer);
    // #transcoding
    IOResult result = streamCompletion.toCompletableFuture().get(1, TimeUnit.SECONDS);
    assertTrue(result.wasSuccessful());
  }
}
