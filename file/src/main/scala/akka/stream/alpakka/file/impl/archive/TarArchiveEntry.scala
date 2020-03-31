/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.file.impl.archive

import java.lang.Long.toOctalString

import akka.annotation.InternalApi
import akka.stream.alpakka.file.TarArchiveMetadata
import akka.util.ByteString

/**
 * INTERNAL API
 */
@InternalApi private[file] final class TarArchiveEntry(metadata: TarArchiveMetadata) {

  def headerBytes: ByteString = {
    val withoutChecksum = headerBytesWithoutChecksum
    val checksumLong = withoutChecksum.foldLeft(0L)((sum, byte) => sum + byte)
    val checksumBytes = ByteString(toOctalString(checksumLong).reverse.padTo(6, '0').take(6).reverse) ++ ByteString(
        new Array[Byte](1) ++ ByteString(" ")
      )
    val withChecksum = withoutChecksum.take(148) ++ checksumBytes ++ withoutChecksum.drop(148 + 8)
    withChecksum.compact
  }

  def trailingBytes: ByteString = {
    val paddingSize = if (metadata.size % 512 > 0) (512 - metadata.size % 512).toInt else 0
    padded(ByteString.empty, paddingSize)
  }

  private def headerBytesWithoutChecksum: ByteString = {
    // [0, 100)
    val fileNameBytes = padded(ByteString(metadata.filePathName), 100)
    // [100, 108)
    val fileModeBytes = padded(ByteString("0755"), 8)
    // [108, 116)
    val ownerIdBytes = padded(ByteString.empty, 8)
    // [116, 124)
    val groupIdBytes = padded(ByteString.empty, 8)
    // [124, 136)
    val fileSizeBytes = padded(ByteString("0" + toOctalString(metadata.size)), 12)
    // [136, 148)
    val lastModificationBytes = padded(ByteString(toOctalString(metadata.lastModification.getEpochSecond)), 12)
    // [148, 156)
    val checksumPlaceholderBytes = ByteString("        ")
    // [156, 157)
    val linkIndicatorBytes = padded(ByteString.empty, 1)
    // [157, 257)
    val linkFileNameBytes = padded(ByteString.empty, 100)
    // [257, 263)
    val ustarIndicatorBytes = ByteString("ustar") ++ ByteString(new Array[Byte](1))
    // [263, 265)
    val ustarVersionBytes = ByteString(new Array[Byte](2))
    // [265, 297)
    val ownerNameBytes = padded(ByteString.empty, 32)
    // [297, 329)
    val groupNameBytes = padded(ByteString.empty, 32)
    // [329, 337)
    val deviceMajorNumberBytes = padded(ByteString.empty, 8)
    // [337, 345)
    val deviceMinorNumberBytes = padded(ByteString.empty, 8)
    // [345, 500)
    val fileNamePrefixBytes = padded(metadata.filePathPrefix.map(ByteString.apply).getOrElse(ByteString.empty), 155)

    padded(
      fileNameBytes ++ fileModeBytes ++ ownerIdBytes ++ groupIdBytes ++ fileSizeBytes ++ lastModificationBytes ++ checksumPlaceholderBytes ++ linkIndicatorBytes ++ linkFileNameBytes ++ ustarIndicatorBytes ++ ustarVersionBytes ++ ownerNameBytes ++ groupNameBytes ++ deviceMajorNumberBytes ++ deviceMinorNumberBytes ++ fileNamePrefixBytes,
      512
    )
  }

  private def padded(bytes: ByteString, targetSize: Int): ByteString = {
    require(bytes.size <= targetSize)
    if (bytes.size < targetSize) bytes ++ ByteString(new Array[Byte](targetSize - bytes.size))
    else bytes
  }

}
