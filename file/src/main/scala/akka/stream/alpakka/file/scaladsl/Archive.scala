/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.file.scaladsl

import akka.NotUsed
import akka.stream.alpakka.file.{ArchiveMetadata, TarArchiveMetadata}
import akka.stream.alpakka.file.impl.archive.{TarArchiveManager, TarReaderStage, ZipArchiveManager}
import akka.stream.scaladsl.{Flow, Source}
import akka.util.ByteString

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
