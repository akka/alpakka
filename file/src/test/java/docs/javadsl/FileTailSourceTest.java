/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.javadsl;

import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.japi.pf.PFBuilder;
import akka.stream.ActorMaterializer;
import akka.stream.KillSwitches;
import akka.stream.Materializer;
import akka.stream.UniqueKillSwitch;
import akka.stream.alpakka.file.DirectoryChange;
import akka.stream.alpakka.testkit.javadsl.LogCapturingJunit4;
import akka.stream.javadsl.Keep;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import akka.stream.testkit.TestSubscriber;
import akka.stream.testkit.javadsl.StreamTestKit;
import akka.testkit.javadsl.TestKit;
import akka.util.ByteString;
import com.google.common.jimfs.Configuration;
import com.google.common.jimfs.Jimfs;
import org.junit.*;

import java.io.FileNotFoundException;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Collections;
import java.util.concurrent.TimeoutException;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.nio.file.StandardOpenOption.APPEND;
import static java.nio.file.StandardOpenOption.WRITE;
import static org.junit.Assert.assertEquals;

public class FileTailSourceTest {

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

  private FileSystem fs;

  @Before
  public void setup() {
    fs = Jimfs.newFileSystem(Configuration.unix());
  }

  @Test
  public void canReadAnEntireFile() throws Exception {
    final Path path = fs.getPath("/file");
    final String dataInFile = "a\nb\nc\nd";
    Files.write(path, dataInFile.getBytes(UTF_8));

    final Source<ByteString, NotUsed> source =
        akka.stream.alpakka.file.javadsl.FileTailSource.create(
            path,
            8192, // chunk size
            0, // starting position
            Duration.ofMillis((250)));

    final TestSubscriber.Probe<ByteString> subscriber = TestSubscriber.probe(system);

    final UniqueKillSwitch killSwitch =
        source
            .viaMat(KillSwitches.single(), Keep.right())
            .to(Sink.fromSubscriber(subscriber))
            .run(materializer);

    ByteString result = subscriber.requestNext();
    assertEquals(dataInFile, result.utf8String());

    killSwitch.shutdown();
    subscriber.expectComplete();
  }

  @Test
  public void willReadNewLinesAppendedAfterReadingTheInitialContents() throws Exception {
    final Path path = fs.getPath("/file");
    Files.write(path, "a\n".getBytes(UTF_8));

    final Source<String, NotUsed> source =
        akka.stream.alpakka.file.javadsl.FileTailSource.createLines(
            path,
            8192, // chunk size
            Duration.ofMillis(250),
            "\n",
            StandardCharsets.UTF_8);

    final TestSubscriber.Probe<String> subscriber = TestSubscriber.probe(system);

    final UniqueKillSwitch killSwitch =
        source
            .viaMat(KillSwitches.single(), Keep.right())
            .to(Sink.fromSubscriber(subscriber))
            .run(materializer);

    String result1 = subscriber.requestNext();
    assertEquals("a", result1);

    subscriber.request(1);
    Files.write(path, "b\n".getBytes(UTF_8), WRITE, APPEND);
    assertEquals("b", subscriber.expectNext());

    Files.write(path, "c\n".getBytes(UTF_8), WRITE, APPEND);
    subscriber.request(1);
    assertEquals("c", subscriber.expectNext());

    killSwitch.shutdown();
    subscriber.expectComplete();
  }

  @Test
  public void willCompleteStreamIfFileIsDeleted() throws Exception {
    final Path path = fs.getPath("/file");
    Files.write(path, "a\n".getBytes(UTF_8));

    final TestSubscriber.Probe<String> subscriber = TestSubscriber.probe(system);

    // #shutdown-on-delete

    final Duration checkInterval = Duration.ofSeconds(1);
    final Source<String, NotUsed> fileCheckSource =
        akka.stream.alpakka.file.javadsl.DirectoryChangesSource.create(
                path.getParent(), checkInterval, 8192)
            .mapConcat(
                pair -> {
                  if (pair.first().equals(path) && pair.second() == DirectoryChange.Deletion) {
                    throw new FileNotFoundException();
                  }
                  return Collections.<String>emptyList();
                })
            .recoverWith(
                new PFBuilder<Throwable, Source<String, NotUsed>>()
                    .match(FileNotFoundException.class, t -> Source.empty())
                    .build());

    final Source<String, NotUsed> source =
        akka.stream.alpakka.file.javadsl.FileTailSource.createLines(
                path,
                8192, // chunk size
                Duration.ofMillis(250))
            .merge(fileCheckSource, true);

    // #shutdown-on-delete

    source.to(Sink.fromSubscriber(subscriber)).run(materializer);

    String result1 = subscriber.requestNext();
    assertEquals("a", result1);

    Files.delete(path);

    subscriber.request(1);
    subscriber.expectComplete();
  }

  @Test
  public void willCompleteStreamIfFileIsIdle() throws Exception {
    final Path path = fs.getPath("/file");
    Files.write(path, "a\n".getBytes(UTF_8));

    final TestSubscriber.Probe<String> subscriber = TestSubscriber.probe(system);

    // #shutdown-on-idle-timeout

    Source<String, NotUsed> stream =
        akka.stream.alpakka.file.javadsl.FileTailSource.createLines(
                path,
                8192, // chunk size
                Duration.ofMillis(250))
            .idleTimeout(Duration.ofSeconds(5))
            .recoverWith(
                new PFBuilder<Throwable, Source<String, NotUsed>>()
                    .match(TimeoutException.class, t -> Source.empty())
                    .build());

    // #shutdown-on-idle-timeout

    stream.to(Sink.fromSubscriber(subscriber)).run(materializer);

    String result1 = subscriber.requestNext();
    assertEquals("a", result1);

    Thread.sleep(Duration.ofSeconds(5).toMillis() + 1000);

    subscriber.expectComplete();
  }

  @After
  public void tearDown() throws Exception {
    fs.close();
    fs = null;
    StreamTestKit.assertAllStagesStopped(materializer);
  }

  // small sample of usage, tails the first argument file path
  public static void main(String... args) {
    if (args.length != 1) throw new IllegalArgumentException("Usage: FileTailSourceTest [path]");
    final String path = args[0];

    final ActorSystem system = ActorSystem.create();
    final Materializer materializer = ActorMaterializer.create(system);

    // #simple-lines
    final FileSystem fs = FileSystems.getDefault();
    final Duration pollingInterval = Duration.ofMillis(250);
    final int maxLineSize = 8192;

    final Source<String, NotUsed> lines =
        akka.stream.alpakka.file.javadsl.FileTailSource.createLines(
            fs.getPath(path), maxLineSize, pollingInterval);

    lines.runForeach((line) -> System.out.println(line), materializer);
    // #simple-lines
  }
}
