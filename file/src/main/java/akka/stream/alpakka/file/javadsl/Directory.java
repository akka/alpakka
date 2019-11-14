/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.file.javadsl;

import akka.NotUsed;
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.FlowWithContext;
import akka.stream.javadsl.Source;
import akka.stream.javadsl.StreamConverters;

import java.nio.file.FileVisitOption;
import java.nio.file.Files;
import java.nio.file.Path;

public final class Directory {

  /** List all files in the given directory */
  public static Source<Path, NotUsed> ls(Path directory) {
    return akka.stream.alpakka.file.scaladsl.Directory.ls(directory).asJava();
  }

  /** Recursively list files and directories in the given directory, depth first. */
  public static Source<Path, NotUsed> walk(Path directory) {
    return StreamConverters.fromJavaStream(() -> Files.walk(directory));
  }

  /**
   * Recursively list files and directories in the given directory, depth first, with a maximum
   * directory depth limit and a possibly set of options (See {@link java.nio.file.Files#walk} for
   * details.
   */
  public static Source<Path, NotUsed> walk(
      Path directory, int maxDepth, FileVisitOption... options) {
    return StreamConverters.fromJavaStream(() -> Files.walk(directory, maxDepth, options));
  }

  /** Create local directories, including any parent directories. */
  public static Flow<Path, Path, NotUsed> mkdirs() {
    return akka.stream.alpakka.file.scaladsl.Directory.mkdirs().asJava();
  }

  /**
   * Create local directories, including any parent directories. Passes arbitrary data as context.
   */
  public static <Ctx> FlowWithContext<Path, Ctx, Path, Ctx, NotUsed> mkdirsWithContext() {
    return akka.stream.alpakka.file.scaladsl.Directory.mkdirsWithContext().asJava();
  }
}
