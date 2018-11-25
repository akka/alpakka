/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.storage

import akka.http.scaladsl.model.ContentType

/**
 * Represents an object within Google Cloud Storage.
 *
 * @param kind The kind of item this is, for objects, this is always storage#object
 * @param id The ID of the object, including the bucket name, object name, and generation number
 * @param name The name of the object
 * @param bucket The name of the bucket containing this object
 * @param generation The content generation of this object, used for object versioning
 * @param contentType Content-Type of the object data, if an object is stored without a Content-Type, it is served as application/octet-stream
 * @param size Content-Length of the data in bytes
 * @param etag HTTP 1.1 Entity tag for the object.
 * @param md5Hash MD5 hash of the data; encoded using base64
 * @param crc32c CRC32c checksum, encoded using base64 in big-endian byte order
 * @param mediaLink Media download link
 */
final class StorageObject private (
    val kind: String,
    val id: String,
    val name: String,
    val bucket: String,
    val generation: Long,
    val contentType: ContentType,
    val size: Long,
    val etag: String,
    val md5Hash: String,
    val crc32c: String,
    val mediaLink: String
) {

  /** Java API */
  def getContentType: akka.http.javadsl.model.ContentType = contentType.asInstanceOf[ContentType]

  def withKind(value: String): StorageObject = copy(kind = value)
  def withId(value: String): StorageObject = copy(id = value)
  def withName(value: String): StorageObject = copy(name = value)
  def withBucket(value: String): StorageObject = copy(bucket = value)
  def withGeneration(value: Long): StorageObject = copy(generation = value)

  /** Scala API */
  def withContentType(value: ContentType): StorageObject = copy(contentType = value)

  /** Java API */
  def withContentType(value: akka.http.javadsl.model.ContentType): StorageObject =
    copy(contentType = value.asInstanceOf[ContentType])
  def withSize(value: Long): StorageObject = copy(size = value)
  def withEtag(value: String): StorageObject = copy(etag = value)
  def withMd5Hash(value: String): StorageObject = copy(md5Hash = value)
  def withCrc32c(value: String): StorageObject = copy(crc32c = value)
  def withMediaLink(value: String): StorageObject = copy(mediaLink = value)

  private def copy(
      kind: String = kind,
      id: String = id,
      name: String = name,
      bucket: String = bucket,
      generation: Long = generation,
      contentType: ContentType = contentType,
      size: Long = size,
      etag: String = etag,
      md5Hash: String = md5Hash,
      crc32c: String = crc32c,
      mediaLink: String = mediaLink
  ): StorageObject = new StorageObject(
    kind = kind,
    id = id,
    name = name,
    bucket = bucket,
    generation = generation,
    contentType = contentType,
    size = size,
    etag = etag,
    md5Hash = md5Hash,
    crc32c = crc32c,
    mediaLink = mediaLink
  )

  override def toString =
    "StorageObject(" +
    s"kind=$kind," +
    s"id=$id," +
    s"name=$name," +
    s"bucket=$bucket," +
    s"generation=$generation," +
    s"contentType=$contentType," +
    s"size=$size," +
    s"etag=$etag," +
    s"md5Hash=$md5Hash," +
    s"crc32c=$crc32c," +
    s"mediaLink=$mediaLink" +
    ")"

  override def equals(other: Any): Boolean = other match {
    case that: StorageObject =>
      java.util.Objects.equals(this.kind, that.kind) &&
      java.util.Objects.equals(this.id, that.id) &&
      java.util.Objects.equals(this.name, that.name) &&
      java.util.Objects.equals(this.bucket, that.bucket) &&
      java.util.Objects.equals(this.generation, that.generation) &&
      java.util.Objects.equals(this.contentType, that.contentType) &&
      java.util.Objects.equals(this.size, that.size) &&
      java.util.Objects.equals(this.etag, that.etag) &&
      java.util.Objects.equals(this.md5Hash, that.md5Hash) &&
      java.util.Objects.equals(this.crc32c, that.crc32c) &&
      java.util.Objects.equals(this.mediaLink, that.mediaLink)
    case _ => false
  }

  override def hashCode(): Int =
    java.util.Objects.hash(kind,
                           id,
                           name,
                           bucket,
                           Long.box(generation),
                           contentType,
                           Long.box(size),
                           etag,
                           md5Hash,
                           crc32c,
                           mediaLink)
}
object StorageObject {

  /** Scala API */
  def apply(
      kind: String,
      id: String,
      name: String,
      bucket: String,
      generation: Long,
      contentType: ContentType,
      size: Long,
      etag: String,
      md5Hash: String,
      crc32c: String,
      mediaLink: String
  ): StorageObject = new StorageObject(
    kind,
    id,
    name,
    bucket,
    generation,
    contentType,
    size,
    etag,
    md5Hash,
    crc32c,
    mediaLink
  )

  /** Java API */
  def create(
      kind: String,
      id: String,
      name: String,
      bucket: String,
      generation: Long,
      contentType: akka.http.javadsl.model.ContentType,
      size: Long,
      etag: String,
      md5Hash: String,
      crc32c: String,
      mediaLink: String
  ): StorageObject = new StorageObject(
    kind,
    id,
    name,
    bucket,
    generation,
    contentType.asInstanceOf[ContentType],
    size,
    etag,
    md5Hash,
    crc32c,
    mediaLink
  )
}
