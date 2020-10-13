/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.file.impl.archive

import akka.NotUsed
import akka.annotation.InternalApi
import akka.stream.alpakka.file.TarArchiveMetadata
import akka.stream.scaladsl.{Flow, Source}
import akka.util.ByteString

/**
 * INTERNAL API
 */
@InternalApi private[file] object TarArchiveManager {

  def tarFlow(): Flow[(TarArchiveMetadata, Source[ByteString, _]), ByteString, NotUsed] = {
    Flow[(TarArchiveMetadata, Source[ByteString, Any])]
      .flatMapConcat { case (metadata, stream) =>
        val entry = new TarArchiveEntry(metadata)
        Source
          .single(entry.headerBytes)
          .concat(stream.via(new EnsureByteStreamSize(metadata.size)))
          .concat(Source.single(entry.trailingBytes))
      }
  }

}
