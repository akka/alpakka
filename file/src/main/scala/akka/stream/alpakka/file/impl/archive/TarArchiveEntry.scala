/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.file.impl.archive

import java.lang.Long.toOctalString
import java.lang.Long.parseUnsignedLong
import java.time.Instant

import akka.annotation.InternalApi
import akka.stream.alpakka.file.TarArchiveMetadata
import akka.util.ByteString

/**
 * INTERNAL API
 */
@InternalApi private[file] object TarArchiveEntry {

  val fileNameLength = 100
  val fileModeLength = 8
  val ownerIdLength = 8
  val groupIdLength = 8
  val fileSizeLength = 12
  val lastModificationLength = 12
  val headerChecksumLength = 8
  val linkIndicatorLength = 1
  val linkFileNameLength = 100

  val ustarIndicatorLength = 6
  val ustarVersionLength = 2
  val ownerNameLength = 32
  val groupNameLength = 32
  val deviceMajorNumberLength = 8
  val deviceMinorNumberLength = 8
  val fileNamePrefixLength = 155
  val headerLength = 512

  private val fixedData1 = {
    // [108, 116)
    padded(ByteString("0755"), fileModeLength) ++
    empty(
      // [100, 108)
      ownerIdLength +
      // [116, 124)
      groupIdLength
    )
  }

  private val fixedData2 = {
    // [148, 156)
    ByteString("        ") ++ // headerChecksumLength
    empty(
      // [156, 157)
      linkIndicatorLength +
      // [157, 257)
      linkFileNameLength
    ) ++
    // [257, 263)
    ByteString("ustar") ++
    empty(
      1 +
      // [263, 265)
      ustarVersionLength +
      // [265, 297)
      ownerNameLength +
      // [297, 329)
      groupNameLength +
      // [329, 337)
      deviceMajorNumberLength +
      // [337, 345)
      deviceMinorNumberLength
    )
  }

  private def padded(bytes: ByteString, targetSize: Int): ByteString = {
    require(bytes.size <= targetSize,
            s"the padded data is ${bytes.size} bytes, which does not fit into  $targetSize bytes")
    if (bytes.size < targetSize) bytes ++ empty(targetSize - bytes.size)
    else bytes
  }

  private def empty(size: Int) = {
    ByteString.fromArrayUnsafe(new Array[Byte](size))
  }

  def parse(bs: ByteString): TarArchiveMetadata = {
    require(bs.length >= headerLength, s"the tar archive header is expected to be at least 512 bytes")
    val filename = getString(bs, 0, fileNameLength)
    val fileSizeString =
      getString(bs, fileNameLength + fileModeLength + ownerIdLength + groupIdLength, fileSizeLength)
    val size = parseUnsignedLong(fileSizeString, 8)
    val lastModificationString =
      getString(bs,
                fileNameLength + fileModeLength + ownerIdLength + groupIdLength + fileSizeLength,
                lastModificationLength)
    val lastModification = Instant.ofEpochSecond(parseUnsignedLong(lastModificationString, 8))
    val fileNamePrefix = getString(
      bs,
      fileNameLength + fileModeLength + ownerIdLength + groupIdLength + fileSizeLength +
      lastModificationLength + headerChecksumLength + linkIndicatorLength + linkFileNameLength + ustarIndicatorLength + ustarVersionLength + ownerNameLength + groupNameLength + deviceMajorNumberLength + deviceMinorNumberLength,
      fileNamePrefixLength
    )
    TarArchiveMetadata(fileNamePrefix, filename, size, lastModification)
  }

  private def getString(bs: ByteString, from: Int, maxLength: Int) = {
    val dropped = bs.drop(from)
    val f = Math.min(dropped.indexOf(0.toByte), maxLength)
    dropped.take(f).utf8String
  }

  def trailerLength(metadata: TarArchiveMetadata): Int = {
    val modulo = metadata.size % 512L
    if (modulo > 0L) (512 - modulo).toInt else 0
  }
}

/**
 * INTERNAL API
 */
@InternalApi private[file] final class TarArchiveEntry(metadata: TarArchiveMetadata) {
  import TarArchiveEntry._

  def headerBytes: ByteString = {
    val withoutChecksum = headerBytesWithoutChecksum
    val checksumLong = withoutChecksum.foldLeft(0L)((sum, byte) => sum + byte)
    val checksumBytes = ByteString(toOctalString(checksumLong).reverse.padTo(6, '0').take(6).reverse) ++ ByteString(
        new Array[Byte](1) ++ ByteString(" ")
      )
    val withChecksum = withoutChecksum.take(148) ++ checksumBytes ++ withoutChecksum.drop(148 + 8)
    withChecksum.compact
  }

  def trailingBytes: ByteString = empty(trailerLength(metadata))

  private def headerBytesWithoutChecksum: ByteString = {
    // [0, 100)
    val fileNameBytes = padded(ByteString(metadata.filePathName), fileNameLength)
    // [124, 136)
    val fileSizeBytes = padded(ByteString("0" + toOctalString(metadata.size)), fileSizeLength)
    // [136, 148)
    val lastModificationBytes =
      padded(ByteString(toOctalString(metadata.lastModification.getEpochSecond)), lastModificationLength)
    // [345, 500)
    val fileNamePrefixBytes = padded(ByteString(metadata.filePathPrefix), fileNamePrefixLength)

    padded(
      fileNameBytes ++ fixedData1 ++ fileSizeBytes ++ lastModificationBytes ++ fixedData2 ++ fileNamePrefixBytes,
      headerLength
    )
  }

}
