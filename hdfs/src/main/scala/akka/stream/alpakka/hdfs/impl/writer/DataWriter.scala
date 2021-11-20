/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.hdfs.impl.writer

import akka.annotation.InternalApi
import akka.stream.alpakka.hdfs.FilePathGenerator
import akka.stream.alpakka.hdfs.impl.writer.HdfsWriter._
import akka.util.ByteString
import org.apache.hadoop.fs.{FSDataOutputStream, FileSystem, Path}

/**
 * Internal API
 */
@InternalApi
private[writer] final case class DataWriter(
    override val fs: FileSystem,
    override val pathGenerator: FilePathGenerator,
    maybeTargetPath: Option[Path],
    override val overwrite: Boolean
) extends HdfsWriter[FSDataOutputStream, ByteString] {

  override protected lazy val target: Path =
    getOrCreatePath(maybeTargetPath, createTargetPath(pathGenerator, 0))

  override def sync(): Unit = output.hsync()

  override def write(input: ByteString, separator: Option[Array[Byte]]): Long = {
    val bytes = input.toArray
    output.write(bytes)
    separator.foreach(output.write)
    output.size()
  }

  override def rotate(rotationCount: Long): DataWriter = {
    output.close()
    copy(maybeTargetPath = Some(createTargetPath(pathGenerator, rotationCount)))
  }

  override def create(fs: FileSystem, file: Path): FSDataOutputStream = fs.create(file, overwrite)

}

private[hdfs] object DataWriter {
  def apply(fs: FileSystem, pathGenerator: FilePathGenerator, overwrite: Boolean): DataWriter =
    new DataWriter(fs, pathGenerator, None, overwrite)
}
