/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.javadsl;

// #retrieving-with-unconfirmed-reads

import akka.stream.IOResult;
import akka.stream.alpakka.ftp.FtpSettings;
import akka.stream.alpakka.ftp.javadsl.Sftp;
import akka.stream.javadsl.Source;
import akka.util.ByteString;

import java.util.concurrent.CompletionStage;

public class SftpRetrievingExample {

  public Source<ByteString, CompletionStage<IOResult>> retrieveFromPath(
      String path, SftpSettings settings) throws Exception {
    return Sftp.fromPath(path, settings.withMaxUnconfirmedReads(64));
  }
}
// #retrieving-with-unconfirmed-reads
