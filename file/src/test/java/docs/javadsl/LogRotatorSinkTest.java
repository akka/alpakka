/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.javadsl;

import akka.Done;
import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.japi.function.Creator;
import akka.japi.function.Function;
import akka.stream.ActorMaterializer;
import akka.stream.Materializer;
import akka.stream.alpakka.file.javadsl.LogRotatorSink;
import akka.stream.alpakka.testkit.javadsl.LogCapturingJunit4;
import akka.stream.javadsl.*;
import akka.stream.testkit.javadsl.StreamTestKit;
import akka.testkit.javadsl.TestKit;
import akka.util.ByteString;
import org.junit.*;

import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Optional;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;

public class LogRotatorSinkTest {

  @Rule public final LogCapturingJunit4 logCapturing = new LogCapturingJunit4();

  private static ActorSystem system;
  private static Materializer materializer;

  @BeforeClass
  public static void beforeAll() throws Exception {
    system = ActorSystem.create();
    materializer = ActorMaterializer.create(system);
  }

  @AfterClass
  public static void afterAll() throws Exception {
    TestKit.shutdownActorSystem(system);
  }

  @After
  public void checkForStageLeaks() {
    StreamTestKit.assertAllStagesStopped(materializer);
  }

  @Test
  public void sizeBased() throws Exception {
    // #size
    Creator<Function<ByteString, Optional<Path>>> sizeBasedTriggerCreator =
        () -> {
          long max = 10 * 1024 * 1024;
          final long[] size = new long[] {max};
          return (element) -> {
            if (size[0] + element.size() > max) {
              Path path = Files.createTempFile("out-", ".log");
              size[0] = element.size();
              return Optional.of(path);
            } else {
              size[0] += element.size();
              return Optional.empty();
            }
          };
        };

    Sink<ByteString, CompletionStage<Done>> sizeRotatorSink =
        LogRotatorSink.createFromFunction(sizeBasedTriggerCreator);
    // #size
    CompletionStage<Done> fileSizeCompletion =
        Source.from(Arrays.asList("test1", "test2", "test3", "test4", "test5", "test6"))
            .map(ByteString::fromString)
            .runWith(sizeRotatorSink, materializer);

    assertEquals(
        Done.getInstance(), fileSizeCompletion.toCompletableFuture().get(2, TimeUnit.SECONDS));
  }

  @Test
  public void timeBased() throws Exception {
    // #time
    final Path destinationDir = FileSystems.getDefault().getPath("/tmp");
    final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("'stream-'yyyy-MM-dd_HH'.log'");

    Creator<Function<ByteString, Optional<Path>>> timeBasedTriggerCreator =
        () -> {
          final String[] currentFileName = new String[] {null};
          return (element) -> {
            String newName = LocalDateTime.now().format(formatter);
            if (newName.equals(currentFileName[0])) {
              return Optional.empty();
            } else {
              currentFileName[0] = newName;
              return Optional.of(destinationDir.resolve(newName));
            }
          };
        };

    Sink<ByteString, CompletionStage<Done>> timeBasedSink =
        LogRotatorSink.createFromFunction(timeBasedTriggerCreator);
    // #time

    CompletionStage<Done> fileSizeCompletion =
        Source.from(Arrays.asList("test1", "test2", "test3", "test4", "test5", "test6"))
            .map(ByteString::fromString)
            .runWith(timeBasedSink, materializer);

    assertEquals(
        Done.getInstance(), fileSizeCompletion.toCompletableFuture().get(2, TimeUnit.SECONDS));

    /*
    // #sample
    import akka.stream.alpakka.file.javadsl.LogRotatorSink;

    Creator<Function<ByteString, Optional<Path>>> triggerFunctionCreator = ...;

    // #sample
    */
    Creator<Function<ByteString, Optional<Path>>> triggerFunctionCreator = timeBasedTriggerCreator;

    Source<ByteString, NotUsed> source =
        Source.from(Arrays.asList("test1", "test2", "test3", "test4", "test5", "test6"))
            .map(ByteString::fromString);
    // #sample
    CompletionStage<Done> completion =
        Source.from(Arrays.asList("test1", "test2", "test3", "test4", "test5", "test6"))
            .map(ByteString::fromString)
            .runWith(LogRotatorSink.createFromFunction(triggerFunctionCreator), materializer);

    // GZip compressing the data written
    CompletionStage<Done> compressedCompletion =
        source.runWith(
            LogRotatorSink.withSinkFactory(
                triggerFunctionCreator,
                path ->
                    Flow.of(ByteString.class)
                        .via(Compression.gzip())
                        .toMat(FileIO.toPath(path), Keep.right())),
            materializer);
    // #sample

    assertEquals(Done.getInstance(), completion.toCompletableFuture().get(2, TimeUnit.SECONDS));
    assertEquals(
        Done.getInstance(), compressedCompletion.toCompletableFuture().get(2, TimeUnit.SECONDS));
  }
}
