/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.file.impl.archive

import java.lang.Long.toOctalString
import java.time.Instant

import akka.annotation.InternalApi
import akka.util.ByteString

/**
 * INTERNAL API
 */
@InternalApi private[file] final class TarArchiveHeader(val filePath: String,
                                                        val size: Long,
                                                        val lastModification: Instant = Instant.now) {

  private val filePathSegments: Array[String] = filePath.split("/")
  private val filePathPrefix = Option(filePathSegments.init).filter(_.nonEmpty).map(_.mkString("/"))
  private val filePathName = filePathSegments.last

  require(
    filePathPrefix.forall(fnp => fnp.length > 0 && fnp.length <= 154),
    "File path prefix must be between 1 and 154 characters long"
  )
  require(filePathName.length > 0 && filePathName.length <= 99,
          "File path name must be between 1 and 99 characters long")

  def bytes: ByteString = {
    val withoutChecksum = bytesWithoutChecksum
    val checksumLong = withoutChecksum.foldLeft(0L)((sum, byte) => sum + byte)
    val checksumBytes = ByteString(toOctalString(checksumLong).reverse.padTo(6, '0').take(6).reverse) ++ ByteString(
        new Array[Byte](1) ++ ByteString(" ")
      )
    val withChecksum = withoutChecksum.take(148) ++ checksumBytes ++ withoutChecksum.drop(148 + 8)
    withChecksum.compact
  }

  private def bytesWithoutChecksum: ByteString = {
    // [0, 100)
    val fileNameBytes = withPadding(ByteString(filePathName), 100)
    // [100, 108)
    val fileModeBytes = withPadding(ByteString("0755"), 8)
    // [108, 116)
    val ownerIdBytes = withPadding(ByteString.empty, 8)
    // [116, 124)
    val groupIdBytes = withPadding(ByteString.empty, 8)
    // [124, 136)
    val fileSizeBytes = withPadding(ByteString("0" + toOctalString(size)), 12)
    // [136, 148)
    val lastModificationBytes = withPadding(ByteString(toOctalString(lastModification.getEpochSecond)), 12)
    // [148, 156)
    val checksumPlaceholderBytes = ByteString("        ")
    // [156, 157)
    val linkIndicatorBytes = withPadding(ByteString.empty, 1)
    // [157, 257)
    val linkFileNameBytes = withPadding(ByteString.empty, 100)
    // [257, 263)
    val ustarIndicatorBytes = ByteString("ustar") ++ ByteString(new Array[Byte](1))
    // [263, 265)
    val ustarVersionBytes = ByteString(new Array[Byte](2))
    // [265, 297)
    val ownerNameBytes = withPadding(ByteString.empty, 32)
    // [297, 329)
    val groupNameBytes = withPadding(ByteString.empty, 32)
    // [329, 337)
    val deviceMajorNumberBytes = withPadding(ByteString.empty, 8)
    // [337, 345)
    val deviceMinorNumberBytes = withPadding(ByteString.empty, 8)
    // [345, 500)
    val fileNamePrefixBytes = withPadding(filePathPrefix.map(ByteString.apply).getOrElse(ByteString.empty), 155)

    withPadding(
      fileNameBytes ++ fileModeBytes ++ ownerIdBytes ++ groupIdBytes ++ fileSizeBytes ++ lastModificationBytes ++ checksumPlaceholderBytes ++ linkIndicatorBytes ++ linkFileNameBytes ++ ustarIndicatorBytes ++ ustarVersionBytes ++ ownerNameBytes ++ groupNameBytes ++ deviceMajorNumberBytes ++ deviceMinorNumberBytes ++ fileNamePrefixBytes,
      512
    )
  }

  private def withPadding(bytes: ByteString, targetSize: Int): ByteString = {
    require(bytes.size <= targetSize)
    if (bytes.size < targetSize) bytes ++ ByteString(new Array[Byte](targetSize - bytes.size))
    else bytes
  }

}
