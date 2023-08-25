/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.file.impl.archive

import akka.annotation.InternalApi
import akka.util.ByteString

/**
 * INTERNAL API
 *
 * ArchiveZipFlow operates on ByteString. But it is required to inform ZipOutputStream when each file starts and ends.
 * For this, special starting and ending ByteString is added.
 */
@InternalApi private[file] object FileByteStringSeparators {
  private val startFileWord = "$START$"
  private val endFileWord = "$END$"
  private val separator: Char = '|'

  def createStartingByteString(path: String): ByteString =
    ByteString(s"$startFileWord$separator$path")

  def createEndingByteString(): ByteString =
    ByteString(endFileWord)

  def isStartingByteString(b: ByteString): Boolean =
    b.size >= 7 && b.slice(0, 7).utf8String == startFileWord

  def isEndingByteString(b: ByteString): Boolean =
    b.size == 5 && b.utf8String == endFileWord

  def getPathFromStartingByteString(b: ByteString): String = {
    val splitted = b.utf8String.split(separator)
    if (splitted.length == 1) {
      ""
    } else if (splitted.length == 2) {
      splitted.tail.head
    } else {
      splitted.tail.mkString(separator.toString)
    }
  }
}
