/*
 * Copyright (C) since 2016 Lightbend Inc. <https://akka.io>
 */

package docs.javadsl;

// #retrieving
import akka.stream.IOResult;
import akka.stream.alpakka.ftp.FtpSettings;
import akka.stream.alpakka.ftp.javadsl.Ftp;
import akka.stream.javadsl.Source;
import akka.util.ByteString;
import java.util.concurrent.CompletionStage;

public class FtpRetrievingExample {

  public Source<ByteString, CompletionStage<IOResult>> retrieveFromPath(
      String path, FtpSettings settings) throws Exception {
    return Ftp.fromPath(path, settings);
  }
}
// #retrieving
