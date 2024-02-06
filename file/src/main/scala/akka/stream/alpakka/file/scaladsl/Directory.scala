/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.file.scaladsl

import java.nio.file.{FileVisitOption, Files, Path}

import akka.NotUsed
import akka.stream.ActorAttributes
import akka.stream.scaladsl.{Flow, FlowWithContext, Source, StreamConverters}

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
           fileVisitOptions: immutable.Seq[FileVisitOption] = Nil
  ): Source[Path, NotUsed] = {
    require(Files.isDirectory(directory), s"Path must be a directory, $directory isn't")
    val factory = maxDepth match {
      case None =>
        () => Files.walk(directory, fileVisitOptions: _*)
      case Some(maxDepth) =>
        () => Files.walk(directory, maxDepth, fileVisitOptions: _*)
    }

    StreamConverters.fromJavaStream(factory)
  }

  /**
   * Create local directories, including any parent directories.
   */
  def mkdirs(): Flow[Path, Path, NotUsed] =
    Flow[Path]
      .map(Files.createDirectories(_))
      .addAttributes(ActorAttributes.dispatcher(ActorAttributes.IODispatcher.dispatcher))

  /**
   * Create local directories, including any parent directories.
   * Passes arbitrary data as context.
   */
  def mkdirsWithContext[Ctx](): FlowWithContext[Path, Ctx, Path, Ctx, NotUsed] = {
    val flow = Flow[(Path, Ctx)]
      .map { tuple =>
        Files.createDirectories(tuple._1)
        tuple
      }
      .addAttributes(ActorAttributes.dispatcher(ActorAttributes.IODispatcher.dispatcher))
    FlowWithContext.fromTuples(flow)
  }

}
