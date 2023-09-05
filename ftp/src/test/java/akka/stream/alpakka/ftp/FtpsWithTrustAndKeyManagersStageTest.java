/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.ftp;

import nl.altindag.ssl.util.PemUtils;
import akka.NotUsed;
import akka.stream.IOResult;
import akka.stream.alpakka.ftp.javadsl.Ftps;
import akka.stream.alpakka.testkit.javadsl.LogCapturingJunit4;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import akka.util.ByteString;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import javax.net.ssl.KeyManager;
import javax.net.ssl.TrustManager;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;

public class FtpsWithTrustAndKeyManagersStageTest extends BaseFtpSupport
    implements CommonFtpStageTest {
  private static final String PEM_PATH = "ftpd/pure-ftpd.pem";

  @Rule public final LogCapturingJunit4 logCapturing = new LogCapturingJunit4();

  @Test
  public void listFiles() throws Exception {
    CommonFtpStageTest.super.listFiles();
  }

  public Source<FtpFile, NotUsed> getBrowserSource(String basePath) throws Exception {
    return Ftps.ls(basePath, settings());
  }

  public Source<ByteString, CompletionStage<IOResult>> getIOSource(String path) throws Exception {
    return Ftps.fromPath(path, settings());
  }

  public Sink<ByteString, CompletionStage<IOResult>> getIOSink(String path) throws Exception {
    return Ftps.toPath(path, settings());
  }

  public Sink<FtpFile, CompletionStage<IOResult>> getRemoveSink() throws Exception {
    return Ftps.remove(settings());
  }

  public Sink<FtpFile, CompletionStage<IOResult>> getMoveSink(
      Function<FtpFile, String> destinationPath) throws Exception {
    return Ftps.move(destinationPath, settings());
  }

  private FtpsSettings settings() throws Exception {
    return FtpsSettings.create(InetAddress.getByName(HOSTNAME))
        .withPort(PORT)
        .withCredentials(CREDENTIALS)
        .withBinary(false)
        .withPassiveMode(true)
        .withTrustManager(trustManager())
        .withKeyManager(keyManager());
  }

  private KeyManager keyManager() throws IOException {
    try (InputStream stream = classLoader().getResourceAsStream(PEM_PATH)) {
      return PemUtils.loadIdentityMaterial(stream);
    }
  }

  private TrustManager trustManager() throws IOException {
    try (InputStream stream = classLoader().getResourceAsStream(PEM_PATH)) {
      return PemUtils.loadTrustMaterial(stream);
    }
  }

  private ClassLoader classLoader() {
    return FtpsWithTrustAndKeyManagersStageTest.class.getClassLoader();
  }
}
