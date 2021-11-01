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

final case class ZipArchiveMetadata(name: String) {
  def getName() = name
}
object ZipArchiveMetadata {
  def create(name: String): ZipArchiveMetadata = ZipArchiveMetadata(name)
}

final class TarArchiveMetadata private (
    val filePathPrefix: Option[String],
    val filePathName: String,
    val size: Long,
    val lastModification: Instant,
    /**
     * See constants `TarchiveMetadata.linkIndicatorNormal`
     */
    val linkIndicatorByte: Byte
) {
  val filePath = filePathPrefix match {
    case None => filePathName
    case Some(prefix) => prefix + "/" + filePathName
  }

  def isDirectory: Boolean =
    linkIndicatorByte == TarArchiveMetadata.linkIndicatorDirectory || filePathName.endsWith("/")

  override def equals(obj: Any): Boolean = {
    obj match {
      case that: TarArchiveMetadata =>
        this.filePathPrefix == that.filePathPrefix &&
        this.filePathName == that.filePathName &&
        this.size == that.size &&
        this.lastModification == that.lastModification &&
        this.linkIndicatorByte == that.linkIndicatorByte
      case _ => false
    }
  }

  override def hashCode(): Int =
    Objects.hash(filePathPrefix, filePathName, Long.box(size), lastModification, Byte.box(linkIndicatorByte))

  override def toString: String =
    "TarArchiveMetadata(" +
    s"filePathPrefix=$filePathPrefix," +
    s"filePathName=$filePathName," +
    s"size=$size," +
    s"lastModification=$lastModification," +
    s"linkIndicatorByte=${linkIndicatorByte.toChar})"
}

object TarArchiveMetadata {

  /**
   * Constants for the `linkIndicator` flag.
   */
  val linkIndicatorNormal: Byte = '0'
  val linkIndicatorLink: Byte = '1'
  val linkIndicatorSymLink: Byte = '2'
  val linkIndicatorCharacterDevice: Byte = '3'
  val linkIndicatorBlockDevice: Byte = '4'
  val linkIndicatorDirectory: Byte = '5'
  val linkIndicatorPipe: Byte = '6'
  val linkIndicatorContiguousFile: Byte = '7'

  def apply(filePath: String, size: Long): TarArchiveMetadata = apply(filePath, size, Instant.now)
  def apply(filePath: String, size: Long, lastModification: Instant): TarArchiveMetadata = {
    val filePathSegments = filePath.lastIndexOf("/")
    val filePathPrefix = if (filePathSegments > 0) {
      Some(filePath.substring(0, filePathSegments))
    } else None
    val filePathName = filePath.substring(filePathSegments + 1, filePath.length)
    apply(filePathPrefix, filePathName, size, lastModification, linkIndicatorNormal)
  }

  def apply(filePathPrefix: String, filePathName: String, size: Long, lastModification: Instant): TarArchiveMetadata = {
    apply(if (filePathPrefix.isEmpty) None else Some(filePathPrefix),
          filePathName,
          size,
          lastModification,
          linkIndicatorNormal)
  }

  /**
   * @param linkIndicatorByte See constants eg. `TarchiveMetadata.linkIndicatorNormal`
   */
  def apply(filePathPrefix: String,
            filePathName: String,
            size: Long,
            lastModification: Instant,
            linkIndicatorByte: Byte): TarArchiveMetadata = {
    apply(if (filePathPrefix.isEmpty) None else Some(filePathPrefix),
          filePathName,
          size,
          lastModification,
          linkIndicatorByte)
  }

  private def apply(filePathPrefix: Option[String],
                    filePathName: String,
                    size: Long,
                    lastModification: Instant,
                    linkIndicatorByte: Byte): TarArchiveMetadata = {
    filePathPrefix.foreach { value =>
      require(
        value.length <= 154,
        "File path prefix must be between 1 and 154 characters long"
      )
    }
    require(filePathName.length >= 0 && filePathName.length <= 99,
            s"File path name must be between 0 and 99 characters long, was ${filePathName.length}")

    new TarArchiveMetadata(filePathPrefix,
                           filePathName,
                           size,
                           // tar timestamp granularity is in seconds
                           lastModification.`with`(ChronoField.NANO_OF_SECOND, 0L),
                           linkIndicatorByte)
  }

  def create(filePath: String, size: Long): TarArchiveMetadata = apply(filePath, size, Instant.now)
  def create(filePath: String, size: Long, lastModification: Instant): TarArchiveMetadata =
    apply(filePath, size, lastModification)
  def create(filePathPrefix: String, filePathName: String, size: Long, lastModification: Instant): TarArchiveMetadata =
    apply(filePathPrefix, filePathName, size, lastModification)

  /**
   * @param linkIndicatorByte See constants eg. `TarchiveMetadata.linkIndicatorNormal`
   */
  def create(filePathPrefix: String,
             filePathName: String,
             size: Long,
             lastModification: Instant,
             linkIndicatorByte: Byte): TarArchiveMetadata =
    apply(filePathPrefix, filePathName, size, lastModification, linkIndicatorByte)

  /**
   * Create metadata for a directory entry.
   */
  def directory(filePathName: String): TarArchiveMetadata =
    directory(filePathName, Instant.now())

  /**
   * Create metadata for a directory entry.
   */
  def directory(filePathName: String, lastModification: Instant): TarArchiveMetadata = {
    val n = if (filePathName.endsWith("/")) filePathName else filePathName + "/"
    apply(None, n, size = 0L, lastModification, linkIndicatorDirectory)
  }

}

final class TarReaderException(msg: String) extends Exception(msg)
