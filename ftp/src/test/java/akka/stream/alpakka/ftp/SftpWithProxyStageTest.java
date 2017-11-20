/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.ftp;

import akka.NotUsed;
import akka.stream.IOResult;
import akka.stream.alpakka.ftp.javadsl.Sftp;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import akka.util.ByteString;
import org.apache.mina.util.AvailablePortFinder;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.littleshoot.proxy.HttpProxyServer;
import org.littleshoot.proxy.impl.DefaultHttpProxyServer;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.util.concurrent.CompletionStage;

public class SftpWithProxyStageTest extends SftpSupportImpl implements CommonFtpStageTest {

  private static final int BASE_PORT = 22000;

  private static HttpProxyServer httpProxyServer;

  private static final Integer proxyPort;

  static {
     proxyPort = AvailablePortFinder.getNextAvailable(BASE_PORT+10);
  }

  @BeforeClass
  public static void startHttpProxyServer() {

    httpProxyServer =
            DefaultHttpProxyServer.bootstrap()
                    .withPort(proxyPort)
                    .start();
  }

  @AfterClass
  public static void stopHttpProxyServer() {
    httpProxyServer.abort();
  }

  @Test
  public void listFiles() throws Exception {
    CommonFtpStageTest.super.listFiles();
  }

  @Test
  public void fromPath() throws Exception {
    CommonFtpStageTest.super.fromPath();
  }

  @Test
  public void toPath() throws Exception {
    CommonFtpStageTest.super.toPath();
  }

  public Source<FtpFile, NotUsed> getBrowserSource(String basePath) throws Exception {
    return Sftp.ls(basePath, settingsWithProxy());
  }

  public Source<ByteString, CompletionStage<IOResult>> getIOSource(String path) throws Exception {
    return Sftp.fromPath(path, settingsWithProxy());
  }

  public Sink<ByteString, CompletionStage<IOResult>> getIOSink(String path) throws Exception {
    return Sftp.toPath(path, settingsWithProxy());
  }

  private SftpSettings settingsWithProxy() throws Exception {
    //#create-settings
    final SftpSettings settings = SftpSettings.create(
            InetAddress.getByName("localhost"))
            .withPort(getPort())
            .withCredentials(FtpCredentials.createAnonCredentials())
            .withStrictHostKeyChecking(false)
            .withProxy(new Proxy(Proxy.Type.HTTP, new InetSocketAddress("localhost", proxyPort)));
    //#create-settings
    return settings;
  }

}
