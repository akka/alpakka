/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.javadsl;

// #traversing
import akka.actor.ActorSystem;
import akka.stream.alpakka.ftp.FtpSettings;
import akka.stream.alpakka.ftp.javadsl.Ftp;

public class FtpTraversingExample {

  public void listFiles(String basePath, FtpSettings settings, ActorSystem system)
      throws Exception {
    Ftp.ls(basePath, settings)
        .runForeach(ftpFile -> System.out.println(ftpFile.toString()), system);
  }
}
// #traversing
