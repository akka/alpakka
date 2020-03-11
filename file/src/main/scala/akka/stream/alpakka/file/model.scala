/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.file

final class ArchiveMetadata private (
    val filePath: String
)

object ArchiveMetadata {
  def apply(filePath: String): ArchiveMetadata = new ArchiveMetadata(filePath)
  def create(filePath: String): ArchiveMetadata = new ArchiveMetadata(filePath)
}
