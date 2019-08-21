/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.file

final class ArchiveMetadata private (
    val filePath: String
)

object ArchiveMetadata {
  def apply(filePath: String): ArchiveMetadata = new ArchiveMetadata(filePath)
  def create(filePath: String): ArchiveMetadata = new ArchiveMetadata(filePath)
}
