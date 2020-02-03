/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.hdfs.scaladsl

import akka.NotUsed
import akka.stream.ActorAttributes.IODispatcher
import akka.stream.scaladsl.{Source, StreamConverters}
import akka.stream.{Attributes, IOResult}
import akka.util.ByteString
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.compress.CompressionCodec
import org.apache.hadoop.io.{SequenceFile, Writable}

import scala.concurrent.Future

object HdfsSource {

  /**
   * Scala API: creates a [[Source]] that consumes as [[ByteString]]
   *
   * @param fs Hadoop file system
   * @param path the file to open
   * @param chunkSize the size of each read operation, defaults to 8192
   */
  def data(
      fs: FileSystem,
      path: Path,
      chunkSize: Int = 8192
  ): Source[ByteString, Future[IOResult]] =
    StreamConverters.fromInputStream(() => fs.open(path), chunkSize)

  /**
   * Scala API: creates a [[Source]] that consumes as [[ByteString]]
   *
   * @param fs Hadoop file system
   * @param path the file to open
   * @param codec a streaming compression/decompression pair
   * @param chunkSize the size of each read operation, defaults to 8192
   */
  def compressed(
      fs: FileSystem,
      path: Path,
      codec: CompressionCodec,
      chunkSize: Int = 8192
  ): Source[ByteString, Future[IOResult]] =
    StreamConverters.fromInputStream(() => codec.createInputStream(fs.open(path)), chunkSize)

  /**
   * Scala API: creates a [[Source]] that consumes as [[(K, V]]
   *
   * @param fs Hadoop file system
   * @param path the file to open
   * @param classK a key class
   * @param classV a value class
   */
  def sequence[K <: Writable, V <: Writable](
      fs: FileSystem,
      path: Path,
      classK: Class[K],
      classV: Class[V]
  ): Source[(K, V), NotUsed] = {
    val reader: SequenceFile.Reader = new SequenceFile.Reader(fs.getConf, SequenceFile.Reader.file(path))
    val it = Iterator
      .continually {
        val key = classK.getDeclaredConstructor().newInstance()
        val value = classV.getDeclaredConstructor().newInstance()
        val hasCurrent = reader.next(key, value)
        (hasCurrent, (key, value))
      }
      .takeWhile(_._1)
      .map(_._2)
    Source
      .fromIterator(() => it)
      .addAttributes(Attributes(IODispatcher))
  }

}
