/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.file

import java.time.Instant

final class ArchiveMetadata private (
    val filePath: String
)

object ArchiveMetadata {
  def apply(filePath: String): ArchiveMetadata = new ArchiveMetadata(filePath)
  def create(filePath: String): ArchiveMetadata = new ArchiveMetadata(filePath)
}

final class TarArchiveMetadata private (
    val filePath: String,
    val size: Long,
    val lastModification: Instant
) {
  private val filePathSegments: Array[String] = filePath.split("/")
  val filePathPrefix = Option(filePathSegments.init).filter(_.nonEmpty).map(_.mkString("/"))
  val filePathName = filePathSegments.last

  require(
    filePathPrefix.forall(fnp => fnp.length > 0 && fnp.length <= 154),
    "File path prefix must be between 1 and 154 characters long"
  )
  require(filePathName.length > 0 && filePathName.length <= 99,
          "File path name must be between 1 and 99 characters long")
}

object TarArchiveMetadata {
  def apply(filePath: String, size: Long): TarArchiveMetadata = apply(filePath, size, Instant.now)
  def apply(filePath: String, size: Long, lastModification: Instant): TarArchiveMetadata =
    new TarArchiveMetadata(filePath, size, lastModification)
  def create(filePath: String, size: Long): TarArchiveMetadata = create(filePath, size, Instant.now)
  def create(filePath: String, size: Long, lastModification: Instant): TarArchiveMetadata =
    new TarArchiveMetadata(filePath, size, lastModification)
}
