/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.file.impl.archive

import akka.NotUsed
import akka.annotation.InternalApi
import akka.stream.alpakka.file.ArchiveMetadata
import akka.stream.scaladsl.{Flow, Source}
import akka.util.ByteString

/**
 * INTERNAL API
 */
@InternalApi private[file] object ZipArchiveManager {

  def zipFlow(
      deflateCompression: Option[Int] = None
  ): Flow[(ArchiveMetadata, Source[ByteString, Any]), ByteString, NotUsed] = {
    val archiveZipFlow = new ZipArchiveFlow(deflateCompression)
    Flow[(ArchiveMetadata, Source[ByteString, Any])]
      .flatMapConcat {
        case (metadata, stream) =>
          val prependElem = Source.single(FileByteStringSeparators.createStartingByteString(metadata.filePath))
          val appendElem = Source.single(FileByteStringSeparators.createEndingByteString())
          stream.prepend(prependElem).concat(appendElem)
      }
      .via(archiveZipFlow)
  }

}
