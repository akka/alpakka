/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.ftp;

import akka.NotUsed;
import akka.stream.IOResult;
import akka.stream.alpakka.ftp.RemoteFileSettings.FtpSettings;
import akka.stream.alpakka.ftp.javadsl.Ftp;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import akka.util.ByteString;
import org.junit.Test;
import java.net.InetAddress;
import java.util.concurrent.CompletionStage;

public class FtpStageTest extends PlainFtpSupportImpl implements CommonFtpStageTest {

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

  //#traversing
  public Source<FtpFile, NotUsed> getBrowserSource(String basePath) throws Exception {
    return Ftp.ls(basePath, settings());
  }
  //#traversing

  //#retrieving
  public Source<ByteString, CompletionStage<IOResult>> getIOSource(String path) throws Exception {
    return Ftp.fromPath(path, settings());
  }
  //#retrieving

  //#storing
  public Sink<ByteString, CompletionStage<IOResult>> getIOSink(String path) throws Exception {
    return Ftp.toPath(path, settings());
  }
  //#storing

  private FtpSettings settings() throws Exception {
    //#create-settings
    final FtpSettings settings = new FtpSettings(
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
