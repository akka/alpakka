/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package docs.javadsl;

// #traversing
import akka.NotUsed;
import akka.stream.Materializer;
import akka.stream.alpakka.ftp.FtpFile;
import akka.stream.alpakka.ftp.FtpSettings;
import akka.stream.alpakka.ftp.javadsl.Ftp;
import akka.stream.javadsl.Source;

public class FtpTraversingExample {

  public void listFiles(String basePath, FtpSettings settings, Materializer materializer)
      throws Exception {
    Ftp.ls(basePath, settings)
        .runForeach(ftpFile -> System.out.println(ftpFile.toString()), materializer);
  }
}
// #traversing
