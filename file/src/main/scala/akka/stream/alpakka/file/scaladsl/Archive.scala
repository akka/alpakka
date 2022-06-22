/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.file.scaladsl

import akka.NotUsed
import akka.stream.alpakka.file.{ArchiveMetadata, TarArchiveMetadata, ZipArchiveMetadata}
import akka.stream.alpakka.file.impl.archive.{TarArchiveManager, TarReaderStage, ZipArchiveManager, ZipSource}
import akka.stream.scaladsl.{Flow, Source}
import akka.util.ByteString
import sun.nio.cs.UTF_8

import java.io.File
import java.nio.charset.Charset

/**
 * Scala API.
 */
object Archive {

  /**
   * Flow for compressing multiple files into one ZIP file.
   */
  def zip(): Flow[(ArchiveMetadata, Source[ByteString, Any]), ByteString, NotUsed] =
    ZipArchiveManager.zipFlow()

  /**
   * Flow for reading ZIP files.
   */
  def zipReader(file: File,
                chunkSize: Int,
                fileCharset: Charset = UTF_8.INSTANCE): Source[(ZipArchiveMetadata, Source[ByteString, Any]), NotUsed] =
    Source.fromGraph(new ZipSource(file, chunkSize, fileCharset))
  def zipReader(file: File): Source[(ZipArchiveMetadata, Source[ByteString, Any]), NotUsed] =
    Source.fromGraph(new ZipSource(file, 8192))

  /**
   * Flow for packaging multiple files into one TAR file.
   */
  def tar(): Flow[(TarArchiveMetadata, Source[ByteString, _]), ByteString, NotUsed] =
    TarArchiveManager.tarFlow()

  /**
   * Parse incoming `ByteString`s into tar file entries and sources for the file contents.
   * The file contents sources MUST be consumed to progress reading the file.
   */
  def tarReader(): Flow[ByteString, (TarArchiveMetadata, Source[ByteString, NotUsed]), NotUsed] =
    Flow.fromGraph(new TarReaderStage())
}
