/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.storage

import akka.http.scaladsl.model.ContentType

/**
 * Represents an object within Google Cloud Storage.
 *
 * @param kind                    The kind of item this is, for objects, this is always storage#object
 * @param id                      The ID of the object, including the bucket name, object name, and generation number
 * @param name                    The name of the object
 * @param bucket                  The name of the bucket containing this object
 * @param generation              The content generation of this object, used for object versioning
 * @param contentType             The Content-Type of the object data, if an object is stored without a Content-Type, it is served as application/octet-stream
 * @param size                    The Content-Length of the data in bytes
 * @param etag                    The HTTP 1.1 Entity tag for the object.
 * @param md5Hash                 The MD5 hash of the data; encoded using base64
 * @param crc32c                  The CRC32c checksum, encoded using base64 in big-endian byte order
 * @param mediaLink               The Media download link
 * @param selfLink                The link to this object
 * @param timeCreated             The creation time of the object in RFC 3339 format.
 * @param updated                 The modification time of the object metadata in RFC 3339 format.
 * @param storageClass            The storage class of the object
 * @param contentEncoding         The Content Encoding of the object data
 * @param contentLanguage         The content language of the objcet data
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
    val mediaLink: String,
    val selfLink: String,
    val updated: Long,
    val timeCreated: Long,
    val storageClass: String,
    val contentEncoding: String,
    val contentLanguage: String
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
  def withSelfLink(value: String): StorageObject = copy(selfLink = value)
  def withUpdated(value: Long): StorageObject = copy(updated = value)
  def withTimeCreated(value: Long): StorageObject = copy(timeCreated = value)
  def withStorageClass(value: String): StorageObject = copy(storageClass = value)
  def withContentEncoding(value: String): StorageObject = copy(contentEncoding = value)
  def withContentLanguage(value: String): StorageObject = copy(contentLanguage = value)

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
      mediaLink: String = mediaLink,
      selfLink: String = selfLink,
      updated: Long = updated,
      timeCreated: Long = timeCreated,
      storageClass: String = storageClass,
      contentEncoding: String = contentEncoding,
      contentLanguage: String = contentLanguage
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
    mediaLink = mediaLink,
    selfLink = selfLink,
    updated = updated,
    timeCreated = timeCreated,
    storageClass = storageClass,
    contentEncoding = contentEncoding,
    contentLanguage = contentLanguage
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
    s"mediaLink=$mediaLink," +
    s"selfLink=$selfLink," +
    s"updated=$updated," +
    s"timeCreated=$timeCreated," +
    s"storageClass=$storageClass," +
    s"contentEncoding=$contentEncoding," +
    s"contentLanguage=$contentLanguage" +
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
      java.util.Objects.equals(this.mediaLink, that.mediaLink) &&
      java.util.Objects.equals(this.selfLink, that.selfLink) &&
      java.util.Objects.equals(this.updated, that.updated) &&
      java.util.Objects.equals(this.timeCreated, that.timeCreated) &&
      java.util.Objects.equals(this.storageClass, that.storageClass) &&
      java.util.Objects.equals(this.contentEncoding, that.contentEncoding) &&
      java.util.Objects.equals(this.contentLanguage, that.contentLanguage)
    case _ => false
  }

  override def hashCode(): Int =
    java.util.Objects.hash(
      kind,
      id,
      name,
      bucket,
      Long.box(generation),
      contentType,
      Long.box(size),
      etag,
      md5Hash,
      crc32c,
      mediaLink,
      selfLink,
      Long.box(updated),
      Long.box(timeCreated),
      storageClass,
      contentEncoding,
      contentLanguage
    )
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
      mediaLink: String,
      selfLink: String,
      updated: Long,
      timeCreated: Long,
      storageClass: String,
      contentEncoding: String,
      contentLanguage: String
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
    mediaLink,
    selfLink,
    updated,
    timeCreated,
    storageClass,
    contentEncoding,
    contentLanguage
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
      mediaLink: String,
      selfLink: String,
      updated: Long,
      timeCreated: Long,
      storageClass: String,
      contentEncoding: String,
      contentLanguage: String
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
    mediaLink,
    selfLink,
    updated,
    timeCreated,
    storageClass,
    contentEncoding,
    contentLanguage
  )
}
