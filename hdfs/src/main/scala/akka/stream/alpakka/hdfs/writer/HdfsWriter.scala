/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.hdfs.writer

import akka.stream.alpakka.hdfs.scaladsl.FilePathGenerator
import akka.stream.alpakka.hdfs.writer.HdfsWriter._
import akka.util.ByteString
import org.apache.hadoop.fs.{FileSystem, Path}

/**
 * Internal API
 */
private[hdfs] trait HdfsWriter[W, I] {
  protected lazy val output: W = create(fs, temp)
  protected lazy val temp: Path = tempFromTarget(pathGenerator, target)

  def moveToTarget(): Boolean = {
    if (!fs.exists(target.getParent))
      fs.mkdirs(target.getParent)
    fs.rename(temp, target)
  }

  def sync(): Unit
  def targetFileName: String = target.getName
  def write(input: I, addNewLine: Boolean): Long
  def rotate(rotationCount: Long): HdfsWriter[W, I]

  protected def target: Path
  protected def fs: FileSystem
  protected def pathGenerator: FilePathGenerator
  protected def create(fs: FileSystem, file: Path): W
}

private[writer] object HdfsWriter {

  val NewLineByteArray: Array[Byte] = ByteString(System.getProperty("line.separator")).toArray

  def createTargetPath(generator: FilePathGenerator, c: Long): Path =
    generator(c, System.currentTimeMillis / 1000)

  def tempFromTarget(generator: FilePathGenerator, target: Path): Path =
    new Path(generator.tempDirectory, target.getName)

}
