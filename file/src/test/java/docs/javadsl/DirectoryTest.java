/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package docs.javadsl;

import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.stream.ActorMaterializer;
import akka.stream.Materializer;
// #walk
// #ls
import akka.stream.alpakka.file.javadsl.Directory;
// #ls
import java.nio.file.FileVisitOption;
// #walk
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import akka.stream.testkit.javadsl.StreamTestKit;
import akka.testkit.TestKit;
import com.google.common.jimfs.Configuration;
import com.google.common.jimfs.Jimfs;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import scala.concurrent.duration.FiniteDuration;

import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;

public class DirectoryTest {

  private FileSystem fs;
  private ActorSystem system;
  private Materializer materializer;

  @Before
  public void setup() {
    fs = Jimfs.newFileSystem(Configuration.unix());
    system = ActorSystem.create();
    materializer = ActorMaterializer.create(system);
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

  @After
  public void tearDown() throws Exception {
    fs.close();
    fs = null;
    StreamTestKit.assertAllStagesStopped(materializer);
    TestKit.shutdownActorSystem(system, FiniteDuration.create(10, TimeUnit.SECONDS), true);
    system = null;
    materializer = null;
  }
}
