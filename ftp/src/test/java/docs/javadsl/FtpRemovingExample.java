/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.javadsl;

// #removing
import akka.stream.IOResult;
import akka.stream.alpakka.ftp.FtpFile;
import akka.stream.alpakka.ftp.FtpSettings;
import akka.stream.alpakka.ftp.javadsl.Ftp;
import akka.stream.javadsl.Sink;
import java.util.concurrent.CompletionStage;

public class FtpRemovingExample {

  public Sink<FtpFile, CompletionStage<IOResult>> remove(FtpSettings settings) throws Exception {
    return Ftp.remove(settings);
  }
}
// #removing
