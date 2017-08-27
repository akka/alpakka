/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
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
import static org.junit.Assert.assertEquals;

interface CommonFtpStageTest extends FtpSupport, AkkaSupport {

  Source<FtpFile, NotUsed> getBrowserSource(String basePath) throws Exception;

  Source<ByteString, CompletionStage<IOResult>> getIOSource(String path) throws Exception;

  Sink<ByteString, CompletionStage<IOResult>> getIOSink(String path) throws Exception;

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
    putFileOnFtp(FtpBaseSupport.FTP_ROOT_DIR, fileName);

    final ActorSystem system = getSystem();
    final Materializer materializer = getMaterializer();

    Source<ByteString, CompletionStage<IOResult>> source = getIOSource("/" + fileName);
    Pair<CompletionStage<IOResult>, TestSubscriber.Probe<ByteString>> pairResult =
            source.toMat(TestSink.probe(system), Keep.both()).run(materializer);
    TestSubscriber.Probe<ByteString> probe = pairResult.second();
    probe.request(100).expectNextOrComplete();

    int expectedNumOfBytes = getLoremIpsum().getBytes().length;
    IOResult result = pairResult.first().toCompletableFuture().get(3, TimeUnit.SECONDS);

    assertEquals(IOResult.createSuccessful(expectedNumOfBytes), result);
  }

  default void toPath() throws Exception {
    String fileName = "sample_io";

    final Materializer materializer = getMaterializer();

    final ByteString fileContent = ByteString.fromString(getLoremIpsum());

    Sink<ByteString, CompletionStage<IOResult>> sink = getIOSink("/" + fileName);
    CompletionStage<IOResult> resultCompletionStage =
            Source.single(fileContent).runWith(sink, materializer);

    int expectedNumOfBytes = getLoremIpsum().getBytes().length;
    IOResult result = resultCompletionStage.toCompletableFuture().get(3, TimeUnit.SECONDS);

    byte[] actualStoredContent = getFtpFileContents(FtpBaseSupport.FTP_ROOT_DIR, fileName);

    assertEquals(IOResult.createSuccessful(expectedNumOfBytes), result);
    Assert.assertArrayEquals(actualStoredContent, getLoremIpsum().getBytes());
  }

}
