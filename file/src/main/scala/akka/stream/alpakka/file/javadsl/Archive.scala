/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.file.javadsl

import akka.NotUsed
import akka.stream.alpakka.file.{scaladsl, ArchiveMetadata, TarArchiveMetadata, ZipArchiveMetadata}
import akka.stream.javadsl.Flow
import akka.util.ByteString
import akka.japi.Pair
import akka.stream.alpakka.file.impl.archive.{TarReaderStage, ZipSource}
import akka.stream.javadsl.Source
import sun.nio.cs.UTF_8

import java.io.File
import java.nio.charset.Charset

/**
 * Java API.
 */
object Archive {

  /**
   * Flow for compressing multiple files into one ZIP file.
   */
  def zip(): Flow[Pair[ArchiveMetadata, Source[ByteString, NotUsed]], ByteString, NotUsed] =
    Flow
      .create[Pair[ArchiveMetadata, Source[ByteString, NotUsed]]]()
      .map(func(pair => (pair.first, pair.second.asScala)))
      .via(scaladsl.Archive.zip().asJava)

  /**
   * Flow for reading ZIP files.
   */
  def zipReader(
      file: File,
      chunkSize: Int,
      fileCharset: Charset = UTF_8.INSTANCE
  ): Source[Pair[ZipArchiveMetadata, Source[ByteString, NotUsed]], NotUsed] =
    Source
      .fromGraph(new ZipSource(file, chunkSize, fileCharset))
      .map(func {
        case (metadata, source) =>
          Pair(metadata, source.asJava)
      })
  def zipReader(file: File): Source[Pair[ZipArchiveMetadata, Source[ByteString, NotUsed]], NotUsed] =
    zipReader(file, 8192)

  /**
   * Flow for packaging multiple files into one TAR file.
   */
  def tar(): Flow[Pair[TarArchiveMetadata, Source[ByteString, NotUsed]], ByteString, NotUsed] =
    Flow
      .create[Pair[TarArchiveMetadata, Source[ByteString, NotUsed]]]()
      .map(func(pair => (pair.first, pair.second.asScala)))
      .via(scaladsl.Archive.tar().asJava)

  /**
   * Parse incoming `ByteString`s into tar file entries and sources for the file contents.
   * The file contents sources MUST be consumed to progress reading the file.
   */
  def tarReader(): Flow[ByteString, Pair[TarArchiveMetadata, Source[ByteString, NotUsed]], NotUsed] =
    Flow
      .fromGraph(new TarReaderStage())
      .map(func {
        case (metadata, source) =>
          Pair(metadata, source.asJava)
      })

  private def func[T, R](f: T => R) = new akka.japi.function.Function[T, R] {
    override def apply(param: T): R = f(param)
  }
}
