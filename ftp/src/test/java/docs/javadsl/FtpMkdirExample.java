/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.javadsl;

// #mkdir-source
import akka.Done;
import akka.NotUsed;
import akka.stream.alpakka.ftp.FtpSettings;
import akka.stream.alpakka.ftp.javadsl.Ftp;
import akka.stream.javadsl.Source;

public class FtpMkdirExample {
  public Source<Done, NotUsed> mkdir(
      String parentPath, String directoryName, FtpSettings settings) {
    return Ftp.mkdir(parentPath, directoryName, settings);
  }
}

// #mkdir-source
