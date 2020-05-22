/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.file.impl.archive

import java.time.{Instant, LocalDateTime, ZoneId, ZonedDateTime}

import akka.stream.alpakka.file.TarArchiveMetadata
import akka.util.ByteString
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class TarArchiveEntrySpec extends AnyFlatSpec with Matchers {
  "Metadata entries" should "be created and parsed back" in {
    val filePathPrefix = "dir1/dir2"
    val filename = "thefile.txt"
    val size = 100
    val lastModified = Instant.from(ZonedDateTime.of(LocalDateTime.of(2020, 4, 11, 11, 34), ZoneId.of("CET")))
    val data = TarArchiveMetadata(filePathPrefix, filename, size, lastModified)
    val entry = new TarArchiveEntry(data)
    val header = entry.headerBytes

    val parsed = TarArchiveEntry.parse(header)
    parsed.filePath shouldBe filePathPrefix + "/" + filename
    parsed.size shouldBe size
    parsed.lastModification shouldBe lastModified
  }

  "Header parser" should "handle both space and null character as terminal" in {
    val filePathPrefix = "dir1/dir2"
    val filename = "thefile.txt"
    val size = 100
    val lastModified = Instant.from(ZonedDateTime.of(LocalDateTime.of(2020, 4, 11, 11, 34), ZoneId.of("CET")))
    val data = TarArchiveMetadata(filePathPrefix, filename, size, lastModified)
    val entry = new TarArchiveEntry(data)

    val headerWithNull = entry.headerBytes
    // Change terminal character after size and lastModified field to be space instead of null
    val bytesWithSpace = headerWithNull.toArray.updated(135, ' '.toByte).updated(147, ' '.toByte)
    val headerWithSpace = ByteString(bytesWithSpace)

    val parsedNull = TarArchiveEntry.parse(headerWithNull)
    val parsedSpace = TarArchiveEntry.parse(headerWithSpace)

    parsedNull shouldBe parsedSpace
  }
}
