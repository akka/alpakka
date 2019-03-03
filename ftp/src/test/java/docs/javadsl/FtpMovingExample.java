/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package docs.javadsl;

// #moving
import akka.stream.IOResult;
import akka.stream.alpakka.ftp.FtpFile;
import akka.stream.alpakka.ftp.FtpSettings;
import akka.stream.alpakka.ftp.javadsl.Ftp;
import akka.stream.javadsl.Sink;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;

public class FtpMovingExample {

  public Sink<FtpFile, CompletionStage<IOResult>> move(
      Function<FtpFile, String> destinationPath, FtpSettings settings) throws Exception {
    return Ftp.move(destinationPath, settings);
  }
}
// #moving
