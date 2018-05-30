/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.hdfs.javadsl

import akka.NotUsed
import akka.japi.Pair
import akka.stream.alpakka.hdfs.impl.strategy.{RotationStrategy, SyncStrategy}
import akka.stream.alpakka.hdfs.scaladsl.{HdfsFlow => ScalaHdfsFlow}
import akka.stream.alpakka.hdfs.{HdfsWritingSettings, IncomingMessage, OutgoingMessage, RotationMessage}
import akka.stream.javadsl
import akka.stream.scaladsl.Flow
import akka.util.ByteString
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.io.SequenceFile.CompressionType
import org.apache.hadoop.io.Writable
import org.apache.hadoop.io.compress.CompressionCodec

object HdfsFlow {

  /*
   * Java API: creates a Flow with [[HdfsFlowStage]] for [[FSDataOutputStream]]
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
  ): javadsl.Flow[IncomingMessage[ByteString, NotUsed], RotationMessage, NotUsed] =
    ScalaHdfsFlow.data(fs, syncStrategy, rotationStrategy, settings).asJava

  /*
   * Java API: creates a Flow with [[HdfsFlowStage]] for [[FSDataOutputStream]] with `passThrough` of type `C`
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
  ): javadsl.Flow[IncomingMessage[ByteString, C], OutgoingMessage[C], NotUsed] =
    ScalaHdfsFlow
      .dataWithPassThrough[C](
        fs,
        syncStrategy,
        rotationStrategy,
        settings
      )
      .asJava

  /*
   * Java API: creates a Flow with [[HdfsFlowStage]] for [[CompressionOutputStream]]
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
  ): javadsl.Flow[IncomingMessage[ByteString, NotUsed], RotationMessage, NotUsed] =
    ScalaHdfsFlow.compressed(fs, syncStrategy, rotationStrategy, compressionCodec, settings).asJava

  /*
   * Java API: creates a Flow with [[HdfsFlowStage]] for [[CompressionOutputStream]] with `passThrough` of type `C`
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
  ): javadsl.Flow[IncomingMessage[ByteString, C], OutgoingMessage[C], NotUsed] =
    ScalaHdfsFlow
      .compressedWithPassThrough[C](
        fs,
        syncStrategy,
        rotationStrategy,
        compressionCodec,
        settings
      )
      .asJava

  /*
   * Java API: creates a Flow with [[HdfsFlowStage]] for [[SequenceFile.Writer]] without a compression
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
  ): javadsl.Flow[IncomingMessage[Pair[K, V], NotUsed], RotationMessage, NotUsed] =
    sequenceWithPassThrough[K, V, NotUsed](fs, syncStrategy, rotationStrategy, settings, classK, classV)
      .collect(ScalaHdfsFlow.OnlyRotationMessage)

  /*
   * Java API: creates a Flow with [[HdfsFlowStage]] for [[SequenceFile.Writer]] with a compression
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
  ): javadsl.Flow[IncomingMessage[Pair[K, V], NotUsed], RotationMessage, NotUsed] =
    sequenceWithPassThrough[K, V, NotUsed](
      fs,
      syncStrategy,
      rotationStrategy,
      compressionType,
      compressionCodec,
      settings,
      classK,
      classV
    ).collect(ScalaHdfsFlow.OnlyRotationMessage)

  /*
   * Java API: creates a Flow with [[HdfsFlowStage]] for [[SequenceFile.Writer]]
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
  ): javadsl.Flow[IncomingMessage[Pair[K, V], C], OutgoingMessage[C], NotUsed] =
    Flow[IncomingMessage[Pair[K, V], C]]
      .map(message => message.copy(source = message.source.toScala))
      .via(
        ScalaHdfsFlow
          .sequenceWithPassThrough[K, V, C](
            fs,
            syncStrategy,
            rotationStrategy,
            settings,
            classK,
            classV
          )
      )
      .asJava

  /*
   * Java API: creates a Flow with [[HdfsFlowStage]] for [[SequenceFile.Writer]]
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
  ): javadsl.Flow[IncomingMessage[Pair[K, V], C], OutgoingMessage[C], NotUsed] =
    Flow[IncomingMessage[Pair[K, V], C]]
      .map(message => message.copy(source = message.source.toScala))
      .via(
        ScalaHdfsFlow
          .sequenceWithPassThrough[K, V, C](
            fs,
            syncStrategy,
            rotationStrategy,
            compressionType,
            compressionCodec,
            settings,
            classK,
            classV
          )
      )
      .asJava

}
