/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package docs.javadsl;

import akka.actor.ActorSystem;
import akka.stream.ActorMaterializer;
import akka.stream.Materializer;
import akka.stream.alpakka.stdin.StdinSourceReader;
import akka.stream.alpakka.stdin.javadsl.StdinSource;
import akka.stream.alpakka.stdin.testkit.ReaderFactory;
import akka.stream.javadsl.Sink;
import akka.testkit.javadsl.TestKit;
import org.junit.*;

import java.util.*;
import java.util.concurrent.*;

import static junit.framework.TestCase.fail;

public class StdinSourceTest {

  private static ActorSystem sys;
  private static Materializer mat;

  /** Called before test suite. */
  @BeforeClass
  public static void setUpBeforeClass() {
    sys = ActorSystem.create("StdinSourceTest");
    mat = ActorMaterializer.create(sys);
  }

  @Test
  public void readSingleStringTest() throws Exception {

    final String[] testStrings = {"abc"};

    final StdinSourceReader singleReader =
        ReaderFactory.createStdinSourceReaderFromList(testStrings);

    final CompletableFuture<String> msgs =
        StdinSource.create(singleReader).runWith(Sink.head(), mat).toCompletableFuture();

    Assert.assertEquals(msgs.get(), "abc");
  }

  @Test
  public void readTwoStringTest() throws Exception {

    String[] testStrings = {"abc", "def"};

    final StdinSourceReader doubleReader =
        ReaderFactory.createStdinSourceReaderFromList(testStrings);

    final CompletableFuture<List<String>> msgs =
        StdinSource.create(doubleReader).runWith(Sink.seq(), mat).toCompletableFuture();

    Assert.assertEquals(msgs.get(), List.of("abc", "def"));
  }

  @Test
  public void readThreeStringTest() throws Exception {

    String[] testStrings = {"abc", "def", "ghi"};

    final StdinSourceReader tripleReader =
        ReaderFactory.createStdinSourceReaderFromList(testStrings);

    final CompletableFuture<List<String>> msgs =
        StdinSource.create(tripleReader).runWith(Sink.seq(), mat).toCompletableFuture();

    Assert.assertEquals(msgs.get(), List.of("abc", "def", "ghi"));
  }

  @Test
  public void mapToStringTest() throws Exception {

    String[] testStrings = {"abc", "def", "ghi"};

    final StdinSourceReader stringReader =
        ReaderFactory.createStdinSourceReaderFromList(testStrings);

    final CompletableFuture<List<String>> msgs =
        StdinSource.create(stringReader)
            .map(String::toUpperCase)
            .runWith(Sink.seq(), mat)
            .toCompletableFuture();

    Assert.assertEquals(msgs.get(), List.of("ABC", "DEF", "GHI"));
  }

  @Test
  public void mapToIntTest() throws Exception {

    String[] testStrings = {"1", "2", "3"};

    final StdinSourceReader intReader = ReaderFactory.createStdinSourceReaderFromList(testStrings);

    final CompletableFuture<List<Integer>> msgs =
        StdinSource.create(intReader)
            .map(Integer::parseInt)
            .runWith(Sink.seq(), mat)
            .toCompletableFuture();

    Assert.assertEquals(msgs.get(), List.of(1, 2, 3));
  }

  @Test
  public void stageFailTest() throws Exception {

    final StdinSourceReader exceptionReader =
        ReaderFactory.createStdinSourceReaderThrowsException();

    try {
      StdinSource.create(exceptionReader).runWith(Sink.seq(), mat).toCompletableFuture().get();
      fail("The exceptionReader did not trigger the expected stage exception");
    } catch (ExecutionException ex) {
      Assert.assertEquals(
          "java.util.NoSuchElementException: example reader failure", ex.getMessage());
    }
  }

  /** Called after test suite. */
  @AfterClass
  public static void tearDownAfterClass() {
    TestKit.shutdownActorSystem(sys);
  }
}
