/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package docs.javadsl;

// #storing
import akka.stream.IOResult;
import akka.stream.alpakka.ftp.javadsl.Ftp;
import akka.stream.javadsl.Compression;
import akka.stream.testkit.javadsl.StreamTestKit;
import akka.util.ByteString;
import java.util.concurrent.CompletionStage;
// #storing
import java.io.PrintWriter;
import java.net.InetAddress;
import akka.stream.alpakka.ftp.FtpSettings;
import akka.stream.alpakka.ftp.PlainFtpSupportImpl;
import akka.stream.alpakka.ftp.FtpBaseSupport;
import akka.stream.Materializer;
import akka.stream.javadsl.Source;
import akka.testkit.javadsl.TestKit;
import org.apache.commons.net.PrintCommandListener;
import org.apache.commons.net.ftp.FTPClient;
import org.junit.*;

import static org.junit.Assert.*;

import java.util.concurrent.TimeUnit;

import static org.hamcrest.CoreMatchers.is;

public class FtpWritingTest extends PlainFtpSupportImpl {

  @After
  public void afterEach() {
    StreamTestKit.assertAllStagesStopped(getMaterializer());
    TestKit.shutdownActorSystem(getSystem());
  }

  FtpSettings ftpSettings() throws Exception {
    // #create-settings
    FtpSettings ftpSettings =
        FtpSettings.create(InetAddress.getByName(hostname))
            .withPort(getPort())
            .withBinary(true)
            .withPassiveMode(true)
            // only useful for debugging
            .withConfigureConnectionConsumer(
                (FTPClient ftpClient) -> {
                  ftpClient.addProtocolCommandListener(
                      new PrintCommandListener(new PrintWriter(System.out), true));
                });
    // #create-settings
    return ftpSettings;
  }

  @Test
  public void targetFileShouldBeCreated() throws Exception {
    Materializer materializer = getMaterializer();
    FtpSettings ftpSettings = ftpSettings();
    // #storing

    CompletionStage<IOResult> result =
        Source.single(ByteString.fromString("this is the file contents"))
            .runWith(Ftp.toPath("file.txt", ftpSettings), materializer);
    // #storing

    IOResult ioResult = result.toCompletableFuture().get(5, TimeUnit.SECONDS);
    assertThat(ioResult, is(IOResult.createSuccessful(25)));
    assertTrue(fileExists(FtpBaseSupport.FTP_ROOT_DIR, "file.txt"));
  }

  @Test
  public void gZippedTargetFileShouldBeCreated() throws Exception {
    Materializer materializer = getMaterializer();
    FtpSettings ftpSettings = ftpSettings();
    // #storing

    // Create a gzipped target file
    CompletionStage<IOResult> result =
        Source.single(ByteString.fromString("this is the file contents"))
            .via(Compression.gzip())
            .runWith(Ftp.toPath("file.txt.gz", ftpSettings), materializer);
    // #storing

    IOResult ioResult = result.toCompletableFuture().get(5, TimeUnit.SECONDS);
    assertThat(ioResult, is(IOResult.createSuccessful(50)));
    assertTrue(fileExists(FtpBaseSupport.FTP_ROOT_DIR, "file.txt.gz"));
  }
}
