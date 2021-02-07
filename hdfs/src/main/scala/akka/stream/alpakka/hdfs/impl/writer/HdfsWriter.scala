/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.hdfs.impl.writer

import akka.annotation.InternalApi
import akka.stream.alpakka.hdfs.FilePathGenerator
import akka.stream.alpakka.hdfs.impl.writer.HdfsWriter._
import org.apache.hadoop.fs.{FileSystem, Path}

import java.lang.System.currentTimeMillis

/**
 * Internal API
 */
@InternalApi
private[hdfs] trait HdfsWriter[W, I] {

  protected lazy val output: W = create(fs, temp)

  protected lazy val temp: Path = tempFromTarget(pathGenerator, target())

  def moveToTarget(rotationCount: Long = 0, timestamp: Long): Boolean = {
    val currTarget = target(rotationCount, timestamp)
    if (!fs.exists(currTarget.getParent))
      fs.mkdirs(currTarget.getParent)
    // mimics FileContext#rename(temp, target, Options.Rename.Overwrite) semantics
    if (overwrite) fs.delete(currTarget, false)
    fs.rename(temp, currTarget)
  }

  def sync(): Unit

  def targetPath(rotationCount: Long, timestamp: Long): String = target(rotationCount, timestamp).toString

  def write(input: I, separator: Option[Array[Byte]]): Long

  def rotate(rotationCount: Long): HdfsWriter[W, I]

  protected def target(rotationCount: Long = 0, timestamp: Long = -1): Path

  protected def fs: FileSystem

  protected def overwrite: Boolean

  protected def pathGenerator: FilePathGenerator

  protected def create(fs: FileSystem, file: Path): W

}

/**
 * Internal API
 */
@InternalApi
private[writer] object HdfsWriter {

  def createTargetPath(generator: FilePathGenerator, c: Long, timestamp: Long = -1): Path =
    generator(c, if (generator.newPathForEachFile) timestamp else currentTimeMillis / 1000)

  def tempFromTarget(generator: FilePathGenerator, target: Path): Path =
    new Path(generator.tempDirectory, target.getName)

  def getOrCreatePath(maybePath: Option[Path], default: => Path): Path =
    maybePath.getOrElse(default)

}
