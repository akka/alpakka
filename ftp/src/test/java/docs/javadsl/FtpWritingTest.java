/*
 * Copyright (C) since 2016 Lightbend Inc. <https://akka.io>
 */

package docs.javadsl;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertTrue;
// #storing

import akka.stream.IOResult;
// #storing
import akka.stream.Materializer;
import akka.stream.alpakka.ftp.BaseFtpSupport;
// #create-settings
import akka.stream.alpakka.ftp.FtpSettings;
// #create-settings
// #storing #create-settings
import akka.stream.alpakka.ftp.javadsl.Ftp;
// #storing #create-settings
// #storing
import akka.stream.alpakka.testkit.javadsl.LogCapturingJunit4;
import akka.stream.javadsl.Compression;
// #storing
// #create-settings
import akka.stream.javadsl.Source;
// #create-settings
// #storing
import akka.stream.testkit.javadsl.StreamTestKit;
// #storing
import akka.testkit.javadsl.TestKit;
// #storing
import akka.util.ByteString;
// #storing
import java.io.PrintWriter;
// #create-settings
import java.net.InetAddress;
// #create-settings
// #storing
import java.util.concurrent.CompletionStage;
// #storing
import java.util.concurrent.TimeUnit;
// #create-settings
import org.apache.commons.net.PrintCommandListener;
import org.apache.commons.net.ftp.FTPClient;
// #create-settings
import org.junit.*;

public class FtpWritingTest extends BaseFtpSupport {

  @Rule public final LogCapturingJunit4 logCapturing = new LogCapturingJunit4();

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
