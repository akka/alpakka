/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.file.impl.archive

import akka.NotUsed
import akka.annotation.InternalApi
import akka.stream.alpakka.file.ArchiveMetadataWithSize
import akka.stream.scaladsl.{Flow, Source}
import akka.util.ByteString

/**
 * INTERNAL API
 */
@InternalApi private[file] object TarArchiveManager {

  def tarFlow(): Flow[(ArchiveMetadataWithSize, Source[ByteString, Any]), ByteString, NotUsed] = {
    Flow[(ArchiveMetadataWithSize, Source[ByteString, Any])]
      .flatMapConcat {
        case (metadata, stream) =>
          val header = new TarArchiveHeader(metadata.filePath, metadata.size)
          Source.single(header.bytes).concat(stream).concat(Source.single(padding(metadata.size)))
      }
  }

  private def padding(fileSize: Long): ByteString = {
    val paddingSize = if (fileSize % 512 > 0) (512 - fileSize % 512).toInt else 0
    if (paddingSize > 0) ByteString(new Array[Byte](paddingSize)) else ByteString.empty
  }

}
