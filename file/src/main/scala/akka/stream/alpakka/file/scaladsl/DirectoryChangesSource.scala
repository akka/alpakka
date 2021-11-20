/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.file.scaladsl

import java.nio.file.Path
import java.util.function.BiFunction

import akka.NotUsed
import akka.stream.alpakka.file.DirectoryChange
import akka.stream.scaladsl.Source

import scala.concurrent.duration.FiniteDuration

object DirectoryChangesSource {

  private val tupler = new BiFunction[Path, DirectoryChange, (Path, DirectoryChange)] {
    override def apply(t: Path, u: DirectoryChange): (Path, DirectoryChange) = (t, u)
  }

  /**
   * Watch directory and emit changes as a stream of tuples containing the path and type of change.
   *
   * @param directoryPath Directory to watch
   * @param pollInterval  Interval between polls to the JDK watch service when a push comes in and there was no changes, if
   *                      the JDK implementation is slow, it will not help lowering this
   * @param maxBufferSize Maximum number of buffered directory changes before the stage fails
   */
  def apply(directoryPath: Path,
            pollInterval: FiniteDuration,
            maxBufferSize: Int): Source[(Path, DirectoryChange), NotUsed] =
    Source.fromGraph(
      new akka.stream.alpakka.file.impl.DirectoryChangesSource(directoryPath, pollInterval, maxBufferSize, tupler)
    )

}
