/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.hdfs.javadsl

import akka.NotUsed
import akka.japi.Pair
import akka.stream.alpakka.hdfs.scaladsl.{HdfsSource => ScalaHdfsSource}
import akka.stream.javadsl.Source
import akka.stream.{javadsl, IOResult}
import akka.util.ByteString
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.Writable
import org.apache.hadoop.io.compress.CompressionCodec

import scala.concurrent.Future

object HdfsSource {

  /**
   * Java API: creates a [[Source]] that consumes as [[ByteString]]
   *
   * @param fs file system
   * @param path the file to open
   */
  def data(
      fs: FileSystem,
      path: Path,
  ): javadsl.Source[ByteString, Future[IOResult]] =
    ScalaHdfsSource.data(fs, path).asJava

  /**
   * Java API: creates a [[Source]] that consumes as [[ByteString]]
   *
   * @param fs file system
   * @param path the file to open
   * @param chunkSize the size of each read operation, defaults to 8192
   */
  def data(
      fs: FileSystem,
      path: Path,
      chunkSize: Int
  ): javadsl.Source[ByteString, Future[IOResult]] =
    ScalaHdfsSource.data(fs, path, chunkSize).asJava

  /**
   * Java API: creates a [[Source]] that consumes as [[ByteString]]
   *
   * @param fs file system
   * @param path the file to open
   * @param codec a streaming compression/decompression pair
   */
  def compressed(
      fs: FileSystem,
      path: Path,
      codec: CompressionCodec
  ): javadsl.Source[ByteString, Future[IOResult]] =
    ScalaHdfsSource.compressed(fs, path, codec).asJava

  /**
   * Java API: creates a [[Source]] that consumes as [[ByteString]]
   *
   * @param fs file system
   * @param path the file to open
   * @param codec a streaming compression/decompression pair
   * @param chunkSize the size of each read operation, defaults to 8192
   */
  def compressed(
      fs: FileSystem,
      path: Path,
      codec: CompressionCodec,
      chunkSize: Int = 8192
  ): javadsl.Source[ByteString, Future[IOResult]] =
    ScalaHdfsSource.compressed(fs, path, codec, chunkSize).asJava

  /**
   * Java API: creates a [[Source]] that consumes as [[(K, V]]
   *
   * @param fs file system
   * @param path the file to open
   * @param classK a key class
   * @param classV a value class
   */
  def sequence[K <: Writable, V <: Writable](
      fs: FileSystem,
      path: Path,
      classK: Class[K],
      classV: Class[V]
  ): javadsl.Source[Pair[K, V], NotUsed] =
    ScalaHdfsSource.sequence(fs, path, classK, classV).map { case (k, v) => new Pair(k, v) }.asJava

}
