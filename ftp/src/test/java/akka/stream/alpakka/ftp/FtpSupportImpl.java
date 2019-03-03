/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.ftp;

import org.apache.ftpserver.FtpServer;
import org.apache.ftpserver.FtpServerFactory;
import org.junit.After;
import org.junit.Before;

import java.nio.file.FileSystem;

abstract class FtpSupportImpl extends FtpBaseSupport {

  private FtpServer ftpServer;

  @Before
  public void startServer() {
    try {
      FtpServerFactory factory = createFtpServerFactory(getPort());
      ftpServer = factory.createServer();
      ftpServer.start();
    } catch (Throwable t) {
      throw new RuntimeException(t);
    }
  }

  @After
  public void stopServer() {
    try {
      ftpServer.stop();
      ftpServer = null;
      FileSystem fileSystem = getFileSystem();
      if (fileSystem.isOpen()) {
        fileSystem.close();
      }
    } catch (Throwable t) {
      throw new RuntimeException(t);
    }
  }

  protected abstract FtpServerFactory createFtpServerFactory(Integer port);
}
