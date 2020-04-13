package akka.stream.alpakka.file.impl.archive

import java.time.{Instant, LocalDateTime, ZoneId, ZonedDateTime}

import akka.stream.alpakka.file.TarArchiveMetadata
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
}
