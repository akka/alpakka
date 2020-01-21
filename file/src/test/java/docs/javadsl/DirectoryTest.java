/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package docs.javadsl;

import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.japi.Pair;
import akka.stream.ActorMaterializer;
import akka.stream.Materializer;
// #walk
// #ls
import akka.stream.alpakka.file.javadsl.Directory;
// #ls
import java.nio.file.FileVisitOption;
// #walk
import akka.stream.alpakka.testkit.javadsl.LogCapturingJunit4;
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.FlowWithContext;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import akka.stream.testkit.javadsl.StreamTestKit;
import akka.testkit.javadsl.TestKit;
import com.google.common.jimfs.Configuration;
import com.google.common.jimfs.Jimfs;
import org.junit.*;

import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class DirectoryTest {

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
  public void listFiles() throws Exception {
    final Path dir = fs.getPath("listfiles");
    Files.createDirectories(dir);
    final Path file1 = Files.createFile(dir.resolve("file1"));
    final Path file2 = Files.createFile(dir.resolve("file2"));

    // #ls

    final Source<Path, NotUsed> source = Directory.ls(dir);
    // #ls

    final List<Path> result =
        source.runWith(Sink.seq(), materializer).toCompletableFuture().get(3, TimeUnit.SECONDS);
    assertEquals(result.size(), 2);
    assertEquals(result.get(0), file1);
    assertEquals(result.get(1), file2);
  }

  @Test
  public void walkAFileTree() throws Exception {
    final Path root = fs.getPath("walk");
    Files.createDirectories(root);
    final Path subdir1 = root.resolve("subdir1");
    Files.createDirectories(subdir1);
    final Path file1 = subdir1.resolve("file1");
    Files.createFile(file1);
    final Path subdir2 = root.resolve("subdir2");
    Files.createDirectories(subdir2);
    final Path file2 = subdir2.resolve("file2");
    Files.createFile(file2);

    // #walk

    final Source<Path, NotUsed> source = Directory.walk(root);
    // #walk

    final List<Path> result =
        source.runWith(Sink.seq(), materializer).toCompletableFuture().get(3, TimeUnit.SECONDS);
    assertEquals(result, Arrays.asList(root, subdir1, file1, subdir2, file2));
  }

  @Test
  public void walkAFileTreeWithOptions() throws Exception {
    final Path root = fs.getPath("walk2");
    Files.createDirectories(root);
    final Path subdir1 = root.resolve("subdir1");
    Files.createDirectories(subdir1);
    final Path file1 = subdir1.resolve("file1");
    Files.createFile(file1);
    final Path subdir2 = root.resolve("subdir2");
    Files.createDirectories(subdir2);
    final Path file2 = subdir2.resolve("file2");
    Files.createFile(file2);

    // #walk

    final Source<Path, NotUsed> source = Directory.walk(root, 1, FileVisitOption.FOLLOW_LINKS);
    // #walk

    final List<Path> result =
        source.runWith(Sink.seq(), materializer).toCompletableFuture().get(3, TimeUnit.SECONDS);
    assertEquals(result, Arrays.asList(root, subdir1, subdir2));
  }

  @Test
  public void createDirectories() throws Exception {
    Path dir = fs.getPath("mkdirsJavadsl");
    Files.deleteIfExists(dir);
    Files.createDirectories(dir);
    // #mkdirs
    Flow<Path, Path, NotUsed> flow = Directory.mkdirs();

    CompletionStage<List<Path>> created =
        Source.from(Arrays.asList(dir.resolve("dirA"), dir.resolve("dirB")))
            .via(flow)
            .runWith(Sink.seq(), materializer);
    // #mkdirs

    final List<Path> result = created.toCompletableFuture().get(3, TimeUnit.SECONDS);
    assertTrue(Files.isDirectory(result.get(0)));
    assertTrue(Files.isDirectory(result.get(1)));
  }

  @Test
  public void createDirectoriesWithContect() throws Exception {
    Path dir = fs.getPath("mkdirsWithContextJavadsl");
    Files.deleteIfExists(dir);
    Files.createDirectories(dir);
    // #mkdirs

    FlowWithContext<Path, SomeContext, Path, SomeContext, NotUsed> flowWithContext =
        Directory.mkdirsWithContext();
    // #mkdirs
    CompletionStage<List<Path>> created =
        Source.from(Arrays.asList(dir.resolve("dirA"), dir.resolve("dirB")))
            .asSourceWithContext(ctx -> new SomeContext())
            .via(flowWithContext)
            .asSource()
            .map(Pair::first)
            .runWith(Sink.seq(), materializer);

    final List<Path> result = created.toCompletableFuture().get(3, TimeUnit.SECONDS);
    assertTrue(Files.isDirectory(result.get(0)));
    assertTrue(Files.isDirectory(result.get(1)));
  }

  @After
  public void tearDown() throws Exception {
    fs.close();
    fs = null;
    StreamTestKit.assertAllStagesStopped(materializer);
  }

  static class SomeContext {}
}
