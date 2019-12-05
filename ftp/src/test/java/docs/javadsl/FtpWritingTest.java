/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package docs.javadsl;

// #storing
// #create-settings
import akka.stream.alpakka.ftp.javadsl.Ftp;
// #create-settings
import akka.stream.IOResult;
import akka.stream.alpakka.testkit.javadsl.LogCapturingJunit4;
import akka.stream.javadsl.Compression;
import akka.stream.testkit.javadsl.StreamTestKit;
import akka.util.ByteString;
import java.util.concurrent.CompletionStage;
// #storing
import java.io.PrintWriter;

// #create-settings
import akka.stream.alpakka.ftp.FtpSettings;
import akka.stream.javadsl.Source;
import org.apache.commons.net.PrintCommandListener;
import org.apache.commons.net.ftp.FTPClient;
import java.net.InetAddress;

// #create-settings
import akka.stream.alpakka.ftp.BaseFtpSupport;
import akka.stream.Materializer;
import akka.testkit.javadsl.TestKit;
import org.junit.*;

import static org.junit.Assert.*;

import java.util.concurrent.TimeUnit;

import static org.hamcrest.CoreMatchers.is;

public class FtpWritingTest extends BaseFtpSupport {

  @Rule
  public final LogCapturingJunit4 logCapturing = new LogCapturingJunit4();

  @After
  public void afterEach() {
    StreamTestKit.assertAllStagesStopped(getMaterializer());
    TestKit.shutdownActorSystem(getSystem());
  }

  FtpSettings ftpSettings() throws Exception {
    // #create-settings
    FtpSettings ftpSettings =
        FtpSettings.create(InetAddress.getByName(HOSTNAME))
            .withPort(PORT)
            .withCredentials(CREDENTIALS)
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
    assertTrue(fileExists("file.txt"));
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
    assertTrue(fileExists("file.txt.gz"));
  }
}
