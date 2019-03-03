/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.hdfs.impl.writer

import akka.annotation.InternalApi
import akka.stream.alpakka.hdfs.FilePathGenerator
import akka.stream.alpakka.hdfs.impl.writer.HdfsWriter._
import akka.util.ByteString
import org.apache.commons.io.FilenameUtils
import org.apache.hadoop.fs.{FSDataOutputStream, FileSystem, Path}
import org.apache.hadoop.io.compress.{CodecPool, CompressionCodec, CompressionOutputStream, Compressor}

/**
 * Internal API
 */
@InternalApi
private[writer] final case class CompressedDataWriter(
    fs: FileSystem,
    compressionCodec: CompressionCodec,
    pathGenerator: FilePathGenerator,
    maybeTargetPath: Option[Path],
    overwrite: Boolean
) extends HdfsWriter[FSDataOutputStream, ByteString] {

  protected lazy val target: Path = getOrCreatePath(maybeTargetPath, outputFileWithExtension(0))

  private val compressor: Compressor = CodecPool.getCompressor(compressionCodec, fs.getConf)
  private val cmpOutput: CompressionOutputStream = compressionCodec.createOutputStream(output, compressor)

  require(compressor ne null, "Compressor cannot be null")

  def sync(): Unit = output.hsync()

  def write(input: ByteString, separator: Option[Array[Byte]]): Long = {
    val bytes = input.toArray
    cmpOutput.write(bytes)
    separator.foreach(output.write)
    compressor.getBytesWritten
  }

  def rotate(rotationCount: Long): CompressedDataWriter = {
    cmpOutput.finish()
    output.close()
    copy(maybeTargetPath = Some(outputFileWithExtension(rotationCount)))
  }

  protected def create(fs: FileSystem, file: Path): FSDataOutputStream = fs.create(file, overwrite)

  private def outputFileWithExtension(rotationCount: Long): Path = {
    val candidatePath = createTargetPath(pathGenerator, rotationCount)
    val candidateExtension = s".${FilenameUtils.getExtension(candidatePath.getName)}"
    val codecExtension = compressionCodec.getDefaultExtension
    if (codecExtension != candidateExtension)
      candidatePath.suffix(codecExtension)
    else candidatePath
  }

}

/**
 * Internal API
 */
@InternalApi
private[hdfs] object CompressedDataWriter {
  def apply(
      fs: FileSystem,
      compressionCodec: CompressionCodec,
      pathGenerator: FilePathGenerator,
      overwrite: Boolean
  ): CompressedDataWriter =
    new CompressedDataWriter(fs, compressionCodec, pathGenerator, None, overwrite)
}
