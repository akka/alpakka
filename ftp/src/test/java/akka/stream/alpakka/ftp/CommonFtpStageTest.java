/*
 * Copyright (C) since 2016 Lightbend Inc. <https://akka.io>
 */

package akka.stream.alpakka.ftp;

import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.japi.Pair;
import akka.stream.IOResult;
import akka.stream.Materializer;
import akka.stream.javadsl.Keep;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import akka.stream.testkit.TestSubscriber;
import akka.stream.testkit.javadsl.TestSink;
import akka.util.ByteString;
import org.junit.Assert;

import java.time.Instant;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

interface CommonFtpStageTest extends BaseSupport, AkkaSupport {

  Source<FtpFile, NotUsed> getBrowserSource(String basePath) throws Exception;

  Source<ByteString, CompletionStage<IOResult>> getIOSource(String path) throws Exception;

  Sink<ByteString, CompletionStage<IOResult>> getIOSink(String path) throws Exception;

  Sink<FtpFile, CompletionStage<IOResult>> getRemoveSink() throws Exception;

  Sink<FtpFile, CompletionStage<IOResult>> getMoveSink(Function<FtpFile, String> destinationPath)
      throws Exception;

  default <T> T await(CompletionStage<T> result)
      throws InterruptedException, java.util.concurrent.ExecutionException,
          java.util.concurrent.TimeoutException {
    return result.toCompletableFuture().get(10, TimeUnit.SECONDS);
  }

  default void listFiles() throws Exception {
    final int numFiles = 30;
    final int pageSize = 10;
    final int demand = 40;
    final String basePath = "";

    final ActorSystem system = getSystem();

    // putting test files on the server
    generateFiles(numFiles, pageSize, basePath);

    Source<FtpFile, NotUsed> source = getBrowserSource(basePath);

    Pair<NotUsed, TestSubscriber.Probe<FtpFile>> pairResult =
        source.toMat(TestSink.create(system), Keep.both()).run(system);
    TestSubscriber.Probe<FtpFile> probe = pairResult.second();
    probe.request(demand).expectNextN(numFiles);
    probe.expectComplete();
  }

  default void fromPath() throws Exception {
    String fileName = "sample_io_" + Instant.now().getNano();
    putFileOnFtp(fileName);

    final ActorSystem system = getSystem();

    Source<ByteString, CompletionStage<IOResult>> source = getIOSource(fileName);
    Pair<CompletionStage<IOResult>, TestSubscriber.Probe<ByteString>> pairResult =
        source.toMat(TestSink.create(system), Keep.both()).run(system);
    TestSubscriber.Probe<ByteString> probe = pairResult.second();
    probe.request(100).expectNextOrComplete();

    int expectedNumOfBytes = getDefaultContent().getBytes().length;
    IOResult result = await(pairResult.first());

    assertEquals(IOResult.createSuccessful(expectedNumOfBytes), result);
  }

  default void toPath() throws Exception {
    String fileName = "sample_io_" + Instant.now().getNano();

    final ActorSystem system = getSystem();

    final ByteString fileContent = ByteString.fromString(getDefaultContent());

    Sink<ByteString, CompletionStage<IOResult>> sink = getIOSink("/" + fileName);
    CompletionStage<IOResult> resultCompletionStage =
        Source.single(fileContent).runWith(sink, system);

    int expectedNumOfBytes = getDefaultContent().getBytes().length;
    IOResult result = await(resultCompletionStage);

    byte[] actualStoredContent = getFtpFileContents(fileName);

    assertEquals(IOResult.createSuccessful(expectedNumOfBytes), result);
    Assert.assertArrayEquals(actualStoredContent, getDefaultContent().getBytes());
  }

  default void remove() throws Exception {
    final String fileName = "sample_io_" + Instant.now().getNano();
    putFileOnFtp(fileName);

    final ActorSystem system = getSystem();
    Source<FtpFile, NotUsed> source = getBrowserSource("/");
    // check that the file is listed
    assertEquals(fileName, await(source.runWith(Sink.head(), system)).name());

    Sink<FtpFile, CompletionStage<IOResult>> sink = getRemoveSink();
    CompletionStage<IOResult> resultCompletionStage = source.runWith(sink, system);

    try {
      IOResult result = await(resultCompletionStage);

      Boolean fileExists = fileExists(fileName);

      assertEquals(IOResult.createSuccessful(1), result);
      assertFalse(fileExists);
    } catch (TimeoutException e) {
      Thread.getAllStackTraces()
          .keySet()
          .forEach(
              (t) ->
                  System.out.println(
                      t.getName() + "\nIs Daemon " + t.isDaemon() + "\nIs Alive " + t.isAlive()));
    }
  }

  default void move() throws Exception {
    final String fileName = "sample_io";
    final String fileName2 = "sample_io2";
    putFileOnFtp(fileName);

    final ActorSystem system = getSystem();
    Source<FtpFile, NotUsed> source = getBrowserSource("/");
    // check that the file is listed
    assertEquals(fileName, await(source.runWith(Sink.head(), system)).name());

    Sink<FtpFile, CompletionStage<IOResult>> sink = getMoveSink((ftpFile) -> fileName2);
    CompletionStage<IOResult> resultCompletionStage = source.runWith(sink, system);

    try {
      IOResult result = await(resultCompletionStage);

      assertEquals(IOResult.createSuccessful(1), result);

      assertFalse(fileExists(fileName));

      assertTrue(fileExists(fileName2));
    } catch (TimeoutException e) {
      Thread.getAllStackTraces()
          .keySet()
          .forEach(
              (t) ->
                  System.out.println(
                      t.getName() + "\nIs Daemon " + t.isDaemon() + "\nIs Alive " + t.isAlive()));
    }
  }
}
