/*
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.ftp;

import akka.NotUsed;
import akka.stream.IOResult;
import akka.stream.alpakka.ftp.RemoteFileSettings.SftpSettings;
import akka.stream.alpakka.ftp.javadsl.Sftp;
import akka.stream.javadsl.Source;
import akka.util.ByteString;
import org.junit.Test;

import java.net.InetAddress;
import java.util.concurrent.CompletionStage;

public class SftpSourceTest extends SftpSupportImpl implements CommonFtpSourceTest {

  @Test
  public void listFiles() throws Exception {
    CommonFtpSourceTest.super.listFiles();
  }

  @Test
  public void fromPath() throws Exception {
    CommonFtpSourceTest.super.fromPath();
  }

  public Source<FtpFile, NotUsed> getBrowserSource(String basePath) throws Exception {
    return Sftp.ls(basePath, settings());
  }

  public Source<ByteString, CompletionStage<IOResult>> getIOSource(String path) throws Exception {
    return Sftp.fromPath(getFileSystem().getPath(path), settings());
  }

  private SftpSettings settings() throws Exception {
    //#create-settings
    final SftpSettings settings = new SftpSettings(
            InetAddress.getByName("localhost"),
            getPort(),
            FtpCredentials.createAnonCredentials(),
            false // strictHostKeyChecking
    );
    //#create-settings
    return settings;
  }
}
