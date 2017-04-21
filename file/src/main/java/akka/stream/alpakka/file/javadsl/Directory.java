/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.file.javadsl;

import akka.NotUsed;
import akka.stream.javadsl.Source;

import java.nio.file.Path;

public final class Directory {

  /**
   * List all files in the given directory
   */
  public static Source<Path, NotUsed> ls(Path directory) {
    return akka.stream.alpakka.file.scaladsl.Directory.ls(directory).asJava();
  }

  /**
   * Recursively list files and directories in the given directory, depth first.
   */
  public static Source<Path, NotUsed> walk(Path directory) {
    return akka.stream.alpakka.file.scaladsl.Directory.walk(directory).asJava();
  }
}
