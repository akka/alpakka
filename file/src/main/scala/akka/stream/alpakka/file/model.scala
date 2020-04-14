/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.file

import java.time.Instant
import java.time.temporal.ChronoField
import java.util.Objects

final class ArchiveMetadata private (
    val filePath: String
)

object ArchiveMetadata {
  def apply(filePath: String): ArchiveMetadata = new ArchiveMetadata(filePath)
  def create(filePath: String): ArchiveMetadata = new ArchiveMetadata(filePath)
}

final class TarArchiveMetadata private (
    val filePathPrefix: Option[String],
    val filePathName: String,
    val size: Long,
    val lastModification: Instant
) {
  val filePath =
    if (filePathPrefix.isEmpty) filePathName
    else filePathPrefix + "/" + filePathName

  override def equals(obj: Any): Boolean = {
    obj match {
      case that: TarArchiveMetadata =>
        this.filePath == that.filePath &&
        this.size == that.size &&
        this.lastModification == that.lastModification
      case _ => false
    }
  }

  override def hashCode(): Int = Objects.hash(filePath, Long.box(size), lastModification)

  override def toString: String =
    "TarArchiveMetadata(" +
    s"filePathPrefix=$filePathPrefix," +
    s"filePathName=$filePathName," +
    s"size=$size," +
    s"lastModification=${lastModification})"
}

object TarArchiveMetadata {
  def apply(filePath: String, size: Long): TarArchiveMetadata = apply(filePath, size, Instant.now)
  def apply(filePath: String, size: Long, lastModification: Instant): TarArchiveMetadata = {
    val filePathSegments = filePath.lastIndexOf("/")
    val filePathPrefix = if (filePathSegments > -1) {
      Some(filePath.substring(0, filePathSegments))
    } else None
    val filePathName = filePath.substring(filePathSegments + 1, filePath.length)
    apply(filePathPrefix, filePathName, size, lastModification)
  }

  def apply(filePathPrefix: String, filePathName: String, size: Long, lastModification: Instant): TarArchiveMetadata = {
    apply(Some(filePathPrefix), filePathName, size, lastModification)
  }

  private def apply(filePathPrefix: Option[String],
                    filePathName: String,
                    size: Long,
                    lastModification: Instant): TarArchiveMetadata = {
    filePathPrefix.foreach { value =>
      require(
        value.length <= 154,
        "File path prefix must be between 1 and 154 characters long"
      )
    }
    require(filePathName.length > 0 && filePathName.length <= 99,
            "File path name must be between 1 and 99 characters long")

    new TarArchiveMetadata(filePathPrefix,
                           filePathName,
                           size,
                           // tar timestamp granularity is in seconds
                           lastModification.`with`(ChronoField.NANO_OF_SECOND, 0L))
  }

  def create(filePath: String, size: Long): TarArchiveMetadata = apply(filePath, size, Instant.now)
  def create(filePath: String, size: Long, lastModification: Instant): TarArchiveMetadata =
    apply(filePath, size, lastModification)
}

final class TarReaderException(msg: String) extends Exception(msg)
