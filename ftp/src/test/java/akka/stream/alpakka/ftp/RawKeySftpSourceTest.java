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
import org.junit.Test;

import java.net.InetAddress;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.concurrent.CompletionStage;

public class RawKeySftpSourceTest extends SftpSupportImpl implements CommonFtpStageTest {

  @Test
  public void listFiles() throws Exception {
    CommonFtpStageTest.super.listFiles();
  }

  @Test
  public void fromPath() throws Exception {
    CommonFtpStageTest.super.fromPath();
  }

  public Source<FtpFile, NotUsed> getBrowserSource(String basePath) throws Exception {
    return Sftp.ls(basePath, settings());
  }

  public Source<ByteString, CompletionStage<IOResult>> getIOSource(String path) throws Exception {
    return Sftp.fromPath(path, settings());
  }

  public Sink<ByteString, CompletionStage<IOResult>> getIOSink(String path) throws Exception {
    return Sftp.toPath(path, settings());
  }

  private SftpSettings settings() throws Exception {
    //#create-settings
    final SftpSettings settings = SftpSettings.create(InetAddress.getByName("localhost"))
            .withPort(getPort())
            .withCredentials(new FtpCredentials.NonAnonFtpCredentials("different user and password", "will fail password auth"))
            .withStrictHostKeyChecking(false) // strictHostKeyChecking
            .withSftpIdentity(SftpIdentity.createRawSftpIdentity("id", Files.readAllBytes(Paths.get("ftp/src/test/resources/client.pem")))
    );
    //#create-settings
    return settings;
  }
}
