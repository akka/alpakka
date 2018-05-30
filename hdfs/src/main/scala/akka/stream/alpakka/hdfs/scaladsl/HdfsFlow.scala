/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.hdfs.scaladsl

import akka.NotUsed
import akka.stream.alpakka.hdfs._
import akka.stream.alpakka.hdfs.impl.HdfsFlowStage
import akka.stream.alpakka.hdfs.impl.strategy.{RotationStrategy, SyncStrategy}
import akka.stream.alpakka.hdfs.impl.writer.{CompressedDataWriter, DataWriter, SequenceWriter}
import akka.stream.scaladsl.Flow
import akka.util.ByteString
import org.apache.hadoop.fs.{FSDataOutputStream, FileSystem}
import org.apache.hadoop.io.SequenceFile.CompressionType
import org.apache.hadoop.io.compress.CompressionCodec
import org.apache.hadoop.io.{SequenceFile, Writable}

object HdfsFlow {

  private[hdfs] val OnlyRotationMessage: PartialFunction[OutgoingMessage[_], RotationMessage] = {
    case m: RotationMessage => m
  }

  /*
   * Scala API: creates a Flow with [[HdfsFlowStage]] for [[FSDataOutputStream]]
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
  ): Flow[IncomingMessage[ByteString, NotUsed], RotationMessage, NotUsed] =
    dataWithPassThrough[NotUsed](fs, syncStrategy, rotationStrategy, settings)
      .collect(OnlyRotationMessage)

  /*
   * Scala API: creates a Flow with [[HdfsFlowStage]] for [[FSDataOutputStream]] with `passThrough` of type `C`
   *
   * @param fs HDFS FileSystem
   * @param syncStrategy Sync Strategy
   * @param rotationStrategy Rotation Strategy
   * @param settings Hdfs writing settings
   */
  def dataWithPassThrough[C](
      fs: FileSystem,
      syncStrategy: SyncStrategy,
      rotationStrategy: RotationStrategy,
      settings: HdfsWritingSettings
  ): Flow[IncomingMessage[ByteString, C], OutgoingMessage[C], NotUsed] =
    Flow
      .fromGraph(
        new HdfsFlowStage[FSDataOutputStream, ByteString, C](
          syncStrategy,
          rotationStrategy,
          settings,
          DataWriter(fs, settings.pathGenerator, settings.overwrite)
        )
      )

  /*
   * Scala API: creates a Flow with [[HdfsFlowStage]] for [[CompressionOutputStream]]
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
  ): Flow[IncomingMessage[ByteString, NotUsed], RotationMessage, NotUsed] =
    compressedWithPassThrough[NotUsed](fs, syncStrategy, rotationStrategy, compressionCodec, settings)
      .collect(OnlyRotationMessage)

  /*
   * Scala API: creates a Flow with [[HdfsFlowStage]] for [[CompressionOutputStream]] with `passThrough` of type `C`
   *
   * @param fs HDFS FileSystem
   * @param syncStrategy Sync Strategy
   * @param rotationStrategy Rotation Strategy
   * @param compressionCodec a class encapsulates a streaming compression/decompression pair.
   * @param settings Hdfs writing settings
   */
  def compressedWithPassThrough[C](
      fs: FileSystem,
      syncStrategy: SyncStrategy,
      rotationStrategy: RotationStrategy,
      compressionCodec: CompressionCodec,
      settings: HdfsWritingSettings
  ): Flow[IncomingMessage[ByteString, C], OutgoingMessage[C], NotUsed] =
    Flow
      .fromGraph(
        new HdfsFlowStage[FSDataOutputStream, ByteString, C](
          syncStrategy,
          rotationStrategy,
          settings,
          CompressedDataWriter(
            fs,
            compressionCodec,
            settings.pathGenerator,
            settings.overwrite
          )
        )
      )

  /*
   * Scala API: creates a Flow with [[HdfsFlowStage]] for [[SequenceFile.Writer]] without a compression
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
  ): Flow[IncomingMessage[(K, V), NotUsed], RotationMessage, NotUsed] =
    sequenceWithPassThrough[K, V, NotUsed](fs, syncStrategy, rotationStrategy, settings, classK, classV)
      .collect(OnlyRotationMessage)

  /*
   * Scala API: creates a Flow with [[HdfsFlowStage]] for [[SequenceFile.Writer]] with a compression
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
  ): Flow[IncomingMessage[(K, V), NotUsed], RotationMessage, NotUsed] =
    sequenceWithPassThrough[K, V, NotUsed](
      fs,
      syncStrategy,
      rotationStrategy,
      compressionType,
      compressionCodec,
      settings,
      classK,
      classV
    ).collect(OnlyRotationMessage)

  /*
   * Scala API: creates a Flow with [[HdfsFlowStage]] for [[SequenceFile.Writer]]
   * with `passThrough` of type `C` and without a compression
   *
   * @param fs Hdfs FileSystem
   * @param syncStrategy sync strategy
   * @param rotationStrategy rotation strategy
   * @param settings Hdfs writing settings
   * @param classK a key class
   * @param classV a value class
   */
  def sequenceWithPassThrough[K <: Writable, V <: Writable, C](
      fs: FileSystem,
      syncStrategy: SyncStrategy,
      rotationStrategy: RotationStrategy,
      settings: HdfsWritingSettings,
      classK: Class[K],
      classV: Class[V]
  ): Flow[IncomingMessage[(K, V), C], OutgoingMessage[C], NotUsed] =
    Flow
      .fromGraph(
        new HdfsFlowStage[SequenceFile.Writer, (K, V), C](
          syncStrategy,
          rotationStrategy,
          settings,
          SequenceWriter(fs, classK, classV, settings.pathGenerator)
        )
      )

  /*
   * Scala API: creates a Flow with [[HdfsFlowStage]] for [[SequenceFile.Writer]]
   * with `passThrough` of type `C` and a compression
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
  def sequenceWithPassThrough[K <: Writable, V <: Writable, C](
      fs: FileSystem,
      syncStrategy: SyncStrategy,
      rotationStrategy: RotationStrategy,
      compressionType: CompressionType,
      compressionCodec: CompressionCodec,
      settings: HdfsWritingSettings,
      classK: Class[K],
      classV: Class[V]
  ): Flow[IncomingMessage[(K, V), C], OutgoingMessage[C], NotUsed] =
    Flow
      .fromGraph(
        new HdfsFlowStage[SequenceFile.Writer, (K, V), C](
          syncStrategy,
          rotationStrategy,
          settings,
          SequenceWriter(fs, compressionType, compressionCodec, classK, classV, settings.pathGenerator)
        )
      )
}
