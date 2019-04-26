/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
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

import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

interface CommonStageTest extends BaseSupport, AkkaSupport {

  Source<FtpFile, NotUsed> getBrowserSource(String basePath) throws Exception;

  Source<ByteString, CompletionStage<IOResult>> getIOSource(String path) throws Exception;

  Sink<ByteString, CompletionStage<IOResult>> getIOSink(String path) throws Exception;

  Sink<FtpFile, CompletionStage<IOResult>> getRemoveSink() throws Exception;

  Sink<FtpFile, CompletionStage<IOResult>> getMoveSink(Function<FtpFile, String> destinationPath)
      throws Exception;

  default void listFiles() throws Exception {
    final int numFiles = 30;
    final int pageSize = 10;
    final int demand = 40;
    final String basePath = "";

    final ActorSystem system = getSystem();
    final Materializer materializer = getMaterializer();

    // putting test files on the server
    generateFiles(numFiles, pageSize, basePath);

    Source<FtpFile, NotUsed> source = getBrowserSource(basePath);

    Pair<NotUsed, TestSubscriber.Probe<FtpFile>> pairResult =
        source.toMat(TestSink.probe(system), Keep.both()).run(materializer);
    TestSubscriber.Probe<FtpFile> probe = pairResult.second();
    probe.request(demand).expectNextN(numFiles);
    probe.expectComplete();
  }

  default void fromPath() throws Exception {
    String fileName = "sample_io";
    putFileOnFtp(fileName);

    final ActorSystem system = getSystem();
    final Materializer materializer = getMaterializer();

    Source<ByteString, CompletionStage<IOResult>> source = getIOSource(fileName);
    Pair<CompletionStage<IOResult>, TestSubscriber.Probe<ByteString>> pairResult =
        source.toMat(TestSink.probe(system), Keep.both()).run(materializer);
    TestSubscriber.Probe<ByteString> probe = pairResult.second();
    probe.request(100).expectNextOrComplete();

    int expectedNumOfBytes = getDefaultContent().getBytes().length;
    IOResult result = pairResult.first().toCompletableFuture().get(3, TimeUnit.SECONDS);

    assertEquals(IOResult.createSuccessful(expectedNumOfBytes), result);
  }

  default void toPath() throws Exception {
    String fileName = "sample_io";

    final Materializer materializer = getMaterializer();

    final ByteString fileContent = ByteString.fromString(getDefaultContent());

    Sink<ByteString, CompletionStage<IOResult>> sink = getIOSink("/" + fileName);
    CompletionStage<IOResult> resultCompletionStage =
        Source.single(fileContent).runWith(sink, materializer);

    int expectedNumOfBytes = getDefaultContent().getBytes().length;
    IOResult result = resultCompletionStage.toCompletableFuture().get(3, TimeUnit.SECONDS);

    byte[] actualStoredContent = getFtpFileContents(fileName);

    assertEquals(IOResult.createSuccessful(expectedNumOfBytes), result);
    Assert.assertArrayEquals(actualStoredContent, getDefaultContent().getBytes());
  }

  default void remove() throws Exception {
    final String fileName = "sample_io";
    putFileOnFtp(fileName);

    final Materializer materializer = getMaterializer();
    Source<FtpFile, NotUsed> source = getBrowserSource("/");
    Sink<FtpFile, CompletionStage<IOResult>> sink = getRemoveSink();
    CompletionStage<IOResult> resultCompletionStage = source.runWith(sink, materializer);

    IOResult result = resultCompletionStage.toCompletableFuture().get(3, TimeUnit.SECONDS);

    Boolean fileExists = fileExists(fileName);

    assertEquals(IOResult.createSuccessful(1), result);
    assertFalse(fileExists);
  }

  default void move() throws Exception {
    final String fileName = "sample_io";
    final String fileName2 = "sample_io2";
    putFileOnFtp(fileName);

    final Materializer materializer = getMaterializer();
    Source<FtpFile, NotUsed> source = getBrowserSource("/");
    Sink<FtpFile, CompletionStage<IOResult>> sink = getMoveSink((ftpFile) -> fileName2);
    CompletionStage<IOResult> resultCompletionStage = source.runWith(sink, materializer);

    IOResult result = resultCompletionStage.toCompletableFuture().get(3, TimeUnit.SECONDS);

    assertEquals(IOResult.createSuccessful(1), result);

    assertFalse(fileExists(fileName));

    assertTrue(fileExists(fileName2));
  }
}
