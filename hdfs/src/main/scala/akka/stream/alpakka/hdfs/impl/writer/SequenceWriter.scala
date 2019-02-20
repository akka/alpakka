/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.hdfs.impl.writer

import akka.annotation.InternalApi
import akka.stream.alpakka.hdfs.FilePathGenerator
import akka.stream.alpakka.hdfs.impl.writer.HdfsWriter._
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.SequenceFile.{CompressionType, Writer}
import org.apache.hadoop.io.compress.CompressionCodec
import org.apache.hadoop.io.{SequenceFile, Writable}

/**
 * Internal API
 */
@InternalApi
private[writer] final case class SequenceWriter[K <: Writable, V <: Writable](
    fs: FileSystem,
    writerOptions: Seq[Writer.Option],
    pathGenerator: FilePathGenerator,
    maybeTargetPath: Option[Path]
) extends HdfsWriter[SequenceFile.Writer, (K, V)] {

  protected lazy val target: Path =
    getOrCreatePath(maybeTargetPath, createTargetPath(pathGenerator, 0))

  def sync(): Unit = output.hsync()

  def write(input: (K, V), separator: Option[Array[Byte]]): Long = {
    output.append(input._1, input._2)
    output.getLength
  }

  def rotate(rotationCount: Long): SequenceWriter[K, V] = {
    output.close()
    copy(maybeTargetPath = Some(createTargetPath(pathGenerator, rotationCount)))
  }

  protected def create(fs: FileSystem, file: Path): SequenceFile.Writer = {
    val ops = SequenceFile.Writer.file(file) +: writerOptions
    SequenceFile.createWriter(fs.getConf, ops: _*)
  }

}

/**
 * Internal API
 */
@InternalApi
private[hdfs] object SequenceWriter {
  def apply[K <: Writable, V <: Writable](
      fs: FileSystem,
      classK: Class[K],
      classV: Class[V],
      pathGenerator: FilePathGenerator
  ): SequenceWriter[K, V] =
    new SequenceWriter[K, V](fs, options(classK, classV), pathGenerator, None)

  def apply[K <: Writable, V <: Writable](
      fs: FileSystem,
      compressionType: CompressionType,
      compressionCodec: CompressionCodec,
      classK: Class[K],
      classV: Class[V],
      pathGenerator: FilePathGenerator
  ): SequenceWriter[K, V] =
    new SequenceWriter[K, V](fs, options(compressionType, compressionCodec, classK, classV), pathGenerator, None)

  private def options[K <: Writable, V <: Writable](
      classK: Class[K],
      classV: Class[V]
  ): Seq[Writer.Option] = Seq(
    SequenceFile.Writer.keyClass(classK),
    SequenceFile.Writer.valueClass(classV)
  )

  private def options[K <: Writable, V <: Writable](
      compressionType: CompressionType,
      compressionCodec: CompressionCodec,
      classK: Class[K],
      classV: Class[V]
  ): Seq[Writer.Option] = SequenceFile.Writer.compression(compressionType, compressionCodec) +: options(classK, classV)
}
