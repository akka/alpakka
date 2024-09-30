/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.file.javadsl;

import akka.NotUsed;
import akka.japi.Pair;
import akka.stream.alpakka.file.DirectoryChange;
import akka.stream.javadsl.Source;

import java.nio.file.Path;

import scala.jdk.javaapi.DurationConverters;
/**
 * Watches a file system directory and streams change events from it.
 *
 * <p>Note that the JDK watcher is notoriously slow on some platform (up to 1s after event actually
 * happened on OSX for example)
 */
public final class DirectoryChangesSource {

  /**
   * @param directoryPath Directory to watch
   * @param pollInterval Interval between polls to the JDK watch service when a push comes in and
   *     there was no changes, if the JDK implementation is slow, it will not help lowering this
   * @param maxBufferSize Maximum number of buffered directory changes before the stage fails
   */
  public static Source<Pair<Path, DirectoryChange>, NotUsed> create(
      Path directoryPath, java.time.Duration pollInterval, int maxBufferSize) {
    return Source.fromGraph(
        new akka.stream.alpakka.file.impl.DirectoryChangesSource<>(
            directoryPath,
            DurationConverters.toScala(pollInterval),
            maxBufferSize,
            Pair::apply));
  }
}
