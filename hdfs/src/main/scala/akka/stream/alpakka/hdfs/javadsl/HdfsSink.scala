/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.hdfs.javadsl

import java.util.concurrent.CompletionStage

import akka.stream.alpakka.hdfs.{HdfsWritingSettings, RotationStrategy, SyncStrategy, WriteLog}
import akka.stream.javadsl
import akka.japi.Pair
import akka.util.ByteString
import akka.{Done, NotUsed}
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.io.SequenceFile.CompressionType
import org.apache.hadoop.io.Writable
import org.apache.hadoop.io.compress.CompressionCodec

object HdfsSink {

  /*
   * Java API: creates a Sink with [[HdfsFlowStage]] for [[FSDataOutputStream]]
   *
   * @param fs HDFS FileSystem
   * @param syncStrategy Sync Strategy
   * @param rotationStrategy Rotation Strategy
   * @param settings Hdfs writing settings
   */
  def data(
      fs: FileSystem,
      syncStrategy: SyncStrategy,
      rotationStrategy: RotationStrategy,
      settings: HdfsWritingSettings
  ): javadsl.Sink[ByteString, CompletionStage[Done]] =
    HdfsFlow
      .data(fs, syncStrategy, rotationStrategy, settings)
      .toMat(javadsl.Sink.ignore[WriteLog], javadsl.Keep.right[NotUsed, CompletionStage[Done]])

  /*
   * Java API: creates a Sink with [[HdfsFlowStage]] for [[CompressionOutputStream]]
   *
   * @param fs HDFS FileSystem
   * @param syncStrategy Sync Strategy
   * @param rotationStrategy Rotation Strategy
   * @param compressionCodec a class encapsulates a streaming compression/decompression pair.
   * @param settings Hdfs writing settings
   */
  def compressed(
      fs: FileSystem,
      syncStrategy: SyncStrategy,
      rotationStrategy: RotationStrategy,
      compressionCodec: CompressionCodec,
      settings: HdfsWritingSettings
  ): javadsl.Sink[ByteString, CompletionStage[Done]] =
    HdfsFlow
      .compressed(fs, syncStrategy, rotationStrategy, compressionCodec, settings)
      .toMat(javadsl.Sink.ignore[WriteLog], javadsl.Keep.right[NotUsed, CompletionStage[Done]])

  /*
   * Java API: creates a Sink with [[HdfsFlowStage]] for [[SequenceFile.Writer]] without a compression
   *
   * @param fs Hdfs FileSystem
   * @param syncStrategy sync strategy
   * @param rotationStrategy rotation strategy
   * @param settings Hdfs writing settings
   * @param classK a key class
   * @param classV a value class
   */
  def sequence[K <: Writable, V <: Writable](
      fs: FileSystem,
      syncStrategy: SyncStrategy,
      rotationStrategy: RotationStrategy,
      settings: HdfsWritingSettings,
      classK: Class[K],
      classV: Class[V]
  ): javadsl.Sink[Pair[K, V], CompletionStage[Done]] =
    HdfsFlow
      .sequence(
        fs,
        syncStrategy,
        rotationStrategy,
        settings,
        classK,
        classV
      )
      .toMat(javadsl.Sink.ignore[WriteLog], javadsl.Keep.right[NotUsed, CompletionStage[Done]])

  /*
   * Java API: creates a Sink with [[HdfsFlowStage]] for [[SequenceFile.Writer]] with a compression
   *
   * @param fs Hdfs FileSystem
   * @param syncStrategy sync strategy
   * @param rotationStrategy rotation strategy
   * @param compressionType a compression type used to compress key/value pairs in the SequenceFile
   * @param compressionCodec a class encapsulates a streaming compression/decompression pair.
   * @param settings Hdfs writing settings
   * @param classK a key class
   * @param classV a value class
   */
  def sequence[K <: Writable, V <: Writable](
      fs: FileSystem,
      syncStrategy: SyncStrategy,
      rotationStrategy: RotationStrategy,
      compressionType: CompressionType,
      compressionCodec: CompressionCodec,
      settings: HdfsWritingSettings,
      classK: Class[K],
      classV: Class[V]
  ): javadsl.Sink[Pair[K, V], CompletionStage[Done]] =
    HdfsFlow
      .sequence(
        fs,
        syncStrategy,
        rotationStrategy,
        compressionType,
        compressionCodec,
        settings,
        classK,
        classV
      )
      .toMat(javadsl.Sink.ignore[WriteLog], javadsl.Keep.right[NotUsed, CompletionStage[Done]])

}
