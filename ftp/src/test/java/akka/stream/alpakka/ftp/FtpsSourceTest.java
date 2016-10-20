/*
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.ftp;

import akka.NotUsed;
import akka.stream.IOResult;
import akka.stream.alpakka.ftp.RemoteFileSettings.FtpsSettings;
import akka.stream.alpakka.ftp.javadsl.Ftps;
import akka.stream.javadsl.Source;
import akka.util.ByteString;
import org.junit.Test;
import java.net.InetAddress;
import java.util.concurrent.CompletionStage;

public class FtpsSourceTest extends FtpsSupportImpl implements CommonFtpSourceTest {

  public FtpsSourceTest() {
    setAuthValue("TLS");
    setUseImplicit(false);
  }

  @Test
  public void listFiles() throws Exception {
    CommonFtpSourceTest.super.listFiles();
  }

  @Test
  public void fromPath() throws Exception {
    CommonFtpSourceTest.super.fromPath();
  }

  public Source<FtpFile, NotUsed> getBrowserSource(String basePath) throws Exception {
    return Ftps.ls(basePath, settings());
  }

  public Source<ByteString, CompletionStage<IOResult>> getIOSource(String path) throws Exception {
    return Ftps.fromPath(getFileSystem().getPath(path), settings());
  }

  private FtpsSettings settings() throws Exception {
    //#create-settings
    final FtpsSettings settings = new FtpsSettings(
            InetAddress.getByName("localhost"),
            getPort(),
            FtpCredentials.createAnonCredentials(),
            false, // binary
            true   // passiveMode
    );
    //#create-settings
    return settings;
  }
}
