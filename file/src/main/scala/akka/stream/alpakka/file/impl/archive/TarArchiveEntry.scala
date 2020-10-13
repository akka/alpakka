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

  /* The C structure for a Tar Entry's header is:
   *
   * struct header {
   * char name[100];     // TarConstants.NAMELEN    - offset   0
   * char mode[8];       // TarConstants.MODELEN    - offset 100
   * char uid[8];        // TarConstants.UIDLEN     - offset 108
   * char gid[8];        // TarConstants.GIDLEN     - offset 116
   * char size[12];      // TarConstants.SIZELEN    - offset 124
   * char mtime[12];     // TarConstants.MODTIMELEN - offset 136
   * char chksum[8];     // TarConstants.CHKSUMLEN  - offset 148
   * char linkflag[1];   //                         - offset 156
   * char linkname[100]; // TarConstants.NAMELEN    - offset 157
   * The following fields are only present in new-style POSIX tar archives:
   * char magic[6];      // TarConstants.MAGICLEN   - offset 257
   * char version[2];    // TarConstants.VERSIONLEN - offset 263
   * char uname[32];     // TarConstants.UNAMELEN   - offset 265
   * char gname[32];     // TarConstants.GNAMELEN   - offset 297
   * char devmajor[8];   // TarConstants.DEVLEN     - offset 329
   * char devminor[8];   // TarConstants.DEVLEN     - offset 337
   * char prefix[155];   // TarConstants.PREFIXLEN  - offset 345
   * // Used if "name" field is not long enough to hold the path
   * char pad[12];       // NULs                    - offset 500
   * } header;
   */

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
    ByteString("        ") // headerChecksumLength
  }

  // [156, 157)
  // linkIndicatorLength

  private val fixedData3 = {
    empty(
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
            s"the padded data is ${bytes.size} bytes, which does not fit into  $targetSize bytes"
    )
    if (bytes.size < targetSize) bytes ++ empty(targetSize - bytes.size)
    else bytes
  }

  private def empty(size: Int) = {
    ByteString.fromArrayUnsafe(new Array[Byte](size))
  }

  def parse(bs: ByteString): TarArchiveMetadata = {
    require(bs.length >= headerLength, s"the tar archive header is expected to be at least 512 bytes")
    require(bs.head != 0, "the file name may not be empty")
    val filename = getString(bs, 0, fileNameLength)
    val fileSizeString =
      getString(bs, fileNameLength + fileModeLength + ownerIdLength + groupIdLength, fileSizeLength)
    val size = parseUnsignedLong(fileSizeString, 8)
    val lastModificationString =
      getString(bs,
                fileNameLength + fileModeLength + ownerIdLength + groupIdLength + fileSizeLength,
                lastModificationLength
      )
    val lastModification = Instant.ofEpochSecond(parseUnsignedLong(lastModificationString, 8))
    val linkIndicatorByte = {
      val tmp = bs(
        fileNameLength + fileModeLength + ownerIdLength + groupIdLength + fileSizeLength +
        lastModificationLength + headerChecksumLength
      )
      if (tmp == 0) TarArchiveMetadata.linkIndicatorNormal else tmp
    }
    val fileNamePrefix = getString(
      bs,
      fileNameLength + fileModeLength + ownerIdLength + groupIdLength + fileSizeLength +
      lastModificationLength + headerChecksumLength + linkIndicatorLength + linkFileNameLength + ustarIndicatorLength + ustarVersionLength + ownerNameLength + groupNameLength + deviceMajorNumberLength + deviceMinorNumberLength,
      fileNamePrefixLength
    )
    TarArchiveMetadata(fileNamePrefix, filename, size, lastModification, linkIndicatorByte)
  }

  private def getString(bs: ByteString, from: Int, maxLength: Int) = {
    val dropped = bs.drop(from)
    val f = Math.min(dropped.indexWhere(b => b == 0.toByte || b == ' '.toByte), maxLength)
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
    val fileNamePrefixBytes =
      padded(metadata.filePathPrefix.map(ByteString.apply).getOrElse(ByteString.empty), fileNamePrefixLength)

    padded(
      fileNameBytes ++ fixedData1 ++ fileSizeBytes ++ lastModificationBytes ++ fixedData2 ++
      ByteString(metadata.linkIndicatorByte) ++ fixedData3 ++ fileNamePrefixBytes,
      headerLength
    )
  }

}
