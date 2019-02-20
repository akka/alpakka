/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.file.scaladsl

import java.nio.file.{FileVisitOption, Files, Path}

import akka.NotUsed
import akka.stream.scaladsl.{Source, StreamConverters}

import scala.collection.immutable

object Directory {

  /**
   * List all files in the given directory
   */
  def ls(directory: Path): Source[Path, NotUsed] = {
    require(Files.isDirectory(directory), s"Path must be a directory, $directory isn't")
    StreamConverters.fromJavaStream(() => Files.list(directory))
  }

  /**
   * Recursively list files and directories in the given directory and its subdirectories. Listing is done
   * depth first.
   *
   * @param maxDepth If defined limits the depth of the directory structure to walk through
   * @param fileVisitOptions See `java.nio.files.Files.walk()` for details
   */
  def walk(directory: Path,
           maxDepth: Option[Int] = None,
           fileVisitOptions: immutable.Seq[FileVisitOption] = Nil): Source[Path, NotUsed] = {
    require(Files.isDirectory(directory), s"Path must be a directory, $directory isn't")
    val factory = maxDepth match {
      case None =>
        () =>
          Files.walk(directory, fileVisitOptions: _*)
      case Some(maxDepth) =>
        () =>
          Files.walk(directory, maxDepth, fileVisitOptions: _*)
    }

    StreamConverters.fromJavaStream(factory)
  }

}
