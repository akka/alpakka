/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.hdfs

import akka.stream.alpakka.hdfs.HdfsWritingSettings._
import akka.stream.alpakka.hdfs.scaladsl.FilePathGenerator

final case class HdfsWritingSettings(
    overwrite: Boolean = true,
    newLine: Boolean = false,
    pathGenerator: FilePathGenerator = DefaultFilePathGenerator
) {
  def withOverwrite(overwrite: Boolean): HdfsWritingSettings = copy(overwrite = overwrite)
  def withNewLine(newLine: Boolean): HdfsWritingSettings = copy(newLine = newLine)
  def withPathGenerator(generator: FilePathGenerator): HdfsWritingSettings = copy(pathGenerator = generator)
}

object HdfsWritingSettings {

  private val DefaultFilePathGenerator: FilePathGenerator =
    FilePathGenerator.create((rc: Long, _: Long) => s"/tmp/alpakka/$rc")

  /**
   * Java API
   */
  def create(): HdfsWritingSettings = HdfsWritingSettings()
}
