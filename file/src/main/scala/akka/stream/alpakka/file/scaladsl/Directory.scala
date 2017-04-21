/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.file.scaladsl

import java.nio.file.attribute.BasicFileAttributes
import java.nio.file.{Files, Path}
import java.util.function.BiPredicate

import akka.NotUsed
import akka.stream.scaladsl.{Source, StreamConverters}

object Directory {

  /**
   * List all files in the given directory
   */
  def ls(directory: Path): Source[Path, NotUsed] = {
    require(Files.isDirectory(directory), s"Path must be a directory, $directory isn't")
    StreamConverters.fromJavaStream(() => Files.list(directory))
  }

  /**
   * Recursively list files in the given directory and its subdirectories. Listing is done
   * depth first.
   */
  def walk(directory: Path): Source[Path, NotUsed] = {
    require(Files.isDirectory(directory), s"Path must be a directory, $directory isn't")
    StreamConverters.fromJavaStream(() => Files.walk(directory))
  }

  private def allFilesFilter = new BiPredicate[Path, BasicFileAttributes] {
    override def test(t: Path, u: BasicFileAttributes): Boolean = true
  }

}
