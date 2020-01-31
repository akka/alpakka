/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.javadsl;

// #processAndMove
import akka.NotUsed;
import akka.japi.Pair;
import akka.stream.alpakka.ftp.FtpFile;
import akka.stream.alpakka.ftp.FtpSettings;
import akka.stream.alpakka.ftp.javadsl.Ftp;
import akka.stream.javadsl.FileIO;
import akka.stream.javadsl.RunnableGraph;

import java.nio.file.Files;
import java.util.function.Function;

public class FtpProcessAndMoveExample {

  public RunnableGraph<NotUsed> processAndMove(
      String sourcePath, Function<FtpFile, String> destinationPath, FtpSettings settings)
      throws Exception {
    return Ftp.ls(sourcePath, settings)
        .flatMapConcat(
            ftpFile ->
                Ftp.fromPath(ftpFile.path(), settings).map(data -> new Pair<>(data, ftpFile)))
        .alsoTo(FileIO.toPath(Files.createTempFile("downloaded", "tmp")).contramap(Pair::first))
        .to(Ftp.move(destinationPath, settings).contramap(Pair::second));
  }
}
// #processAndMove
