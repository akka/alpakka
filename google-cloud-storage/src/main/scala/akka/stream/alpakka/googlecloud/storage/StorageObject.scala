/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.storage

import java.time.OffsetDateTime
import java.util.Optional

import akka.http.scaladsl.model.ContentType
import main.scala.akka.stream.alpakka.googlecloud.storage.{CustomerEncryption, ObjectAccessControls, Owner}
import scala.compat.java8.OptionConverters._
import scala.collection.JavaConverters._

/**
 * Represents an object within Google Cloud Storage.
 * Refer to https://cloud.google.com/storage/docs/json_api/v1/objects#resource-representations for more in depth docs
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
 * @param timeDeleted             The deletion time of the object in RFC 3339 format. Returned if and only if this version of the object is no longer a live version, but remains in the bucket as a noncurrent version.
 * @param updated                 The modification time of the object metadata in RFC 3339 format.
 * @param storageClass            The storage class of the object
 * @param contentDisposition      The Content-Disposition of the object data.
 * @param contentEncoding         The Content Encoding of the object data
 * @param contentLanguage         The content language of the objcet data
 * @param metageneration          The version of the metadata for this object at this generation.
 * @param temporaryHold           Whether or not the object is subject to a temporary hold
 * @param eventBasedHold          Whether or not the object is subject to an event-based hold.
 * @param retentionExpirationTime The earliest time that the object can be deleted, based on a bucket's retention policy, in RFC 3339 format.
 * @param timeStorageClassUpdated The time at which the object's storage class was last changed.
 * @param cacheControl            Cache-Control directive for the object data.
 * @param metadata                User-provided metadata, in key/value pairs.
 * @param componentCount          Number of underlying components that make up a composite object.
 * @param kmsKeyName              Cloud KMS Key used to encrypt this object, if the object is encrypted by such a key.
 * @param customerEncryption      Metadata of customer-supplied encryption key, if the object is encrypted by such a key.
 * @param owner                   The owner of the object. This will always be the uploader of the object
 * @param acl                     Access controls on the object, containing one or more objectAccessControls Resources. If iamConfiguration.uniformBucketLevelAccess.enabled is set to true, this field is omitted in responses, and requests that specify this field fail.
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
    val updated: OffsetDateTime,
    val timeCreated: OffsetDateTime,
    val timeDeleted: Option[OffsetDateTime],
    val storageClass: String,
    val contentDisposition: Option[String],
    val contentEncoding: Option[String],
    val contentLanguage: Option[String],
    val metageneration: Long,
    val temporaryHold: Option[Boolean],
    val eventBasedHold: Option[Boolean],
    val retentionExpirationTime: Option[OffsetDateTime],
    val timeStorageClassUpdated: OffsetDateTime,
    val cacheControl: Option[String],
    val metadata: Option[Map[String, String]],
    val componentCount: Option[Int],
    val kmsKeyName: Option[String],
    val customerEncryption: Option[CustomerEncryption],
    val owner: Option[Owner],
    val acl: Option[List[ObjectAccessControls]]
) {

  /** Java API */
  def getContentType: akka.http.javadsl.model.ContentType = contentType.asInstanceOf[ContentType]
  def getTimeDeleted: Optional[OffsetDateTime] = timeDeleted.asJava
  def getContentDisposition: Optional[String] = contentDisposition.asJava
  def getContentEncoding: Optional[String] = contentEncoding.asJava
  def getContentLanguage: Optional[String] = contentLanguage.asJava
  def getTemporaryHold: Optional[Boolean] = temporaryHold.asJava
  def getEventBasedHold: Optional[Boolean] = eventBasedHold.asJava
  def getRetentionExpirationTime: Optional[OffsetDateTime] = retentionExpirationTime.asJava
  def getCacheControl: Optional[String] = cacheControl.asJava
  def getMetadata: Optional[java.util.Map[String, String]] = metadata.map(_.asJava).asJava
  def getComponentCount: Optional[Integer] = componentCount.map(Int.box).asJava
  def getKmsKeyName: Optional[String] = kmsKeyName.asJava
  def getCustomerEncryption: Optional[CustomerEncryption] = customerEncryption.asJava
  def getOwner: Optional[Owner] = owner.asJava
  def getAcl: Optional[java.util.List[ObjectAccessControls]] = acl.map(_.asJava).asJava

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
  def withUpdated(value: OffsetDateTime): StorageObject = copy(updated = value)
  def withTimeCreated(value: OffsetDateTime): StorageObject = copy(timeCreated = value)
  def withTimeDeleted(value: OffsetDateTime): StorageObject = copy(timeDeleted = Option(value))
  def withStorageClass(value: String): StorageObject = copy(storageClass = value)
  def withContentDisposition(value: String): StorageObject = copy(contentDisposition = Option(value))
  def withContentEncoding(value: String): StorageObject = copy(contentEncoding = Option(value))
  def withContentLanguage(value: String): StorageObject = copy(contentLanguage = Option(value))
  def withMetageneration(value: Long): StorageObject = copy(metageneration = value)
  def withTemporaryHold(value: Boolean): StorageObject = copy(temporaryHold = Option(value))
  def withEventBasedHold(value: Boolean): StorageObject = copy(eventBasedHold = Option(value))
  def withRetentionExpirationTime(value: OffsetDateTime): StorageObject = copy(retentionExpirationTime = Option(value))
  def withTimeStorageClassUpdated(value: OffsetDateTime): StorageObject = copy(timeStorageClassUpdated = value)
  def withCacheControl(value: String): StorageObject = copy(cacheControl = Option(value))
  def withMetadata(value: Map[String, String]): StorageObject = copy(metadata = Option(value))
  def withComponentCount(value: Int): StorageObject = copy(componentCount = Option(value))
  def withKmsKeyName(value: String): StorageObject = copy(kmsKeyName = Option(value))
  def withCustomerEncryption(value: CustomerEncryption): StorageObject = copy(customerEncryption = Option(value))
  def withOwner(value: Owner): StorageObject = copy(owner = Option(value))
  def withAcl(value: List[ObjectAccessControls]): StorageObject = copy(acl = Option(value))

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
      updated: OffsetDateTime = updated,
      timeCreated: OffsetDateTime = timeCreated,
      timeDeleted: Option[OffsetDateTime] = timeDeleted,
      storageClass: String = storageClass,
      contentDisposition: Option[String] = contentDisposition,
      contentEncoding: Option[String] = contentEncoding,
      contentLanguage: Option[String] = contentLanguage,
      metageneration: Long = metageneration,
      temporaryHold: Option[Boolean] = temporaryHold,
      eventBasedHold: Option[Boolean] = eventBasedHold,
      retentionExpirationTime: Option[OffsetDateTime] = retentionExpirationTime,
      timeStorageClassUpdated: OffsetDateTime = timeStorageClassUpdated,
      cacheControl: Option[String] = cacheControl,
      metadata: Option[Map[String, String]] = metadata,
      componentCount: Option[Int] = componentCount,
      kmsKeyName: Option[String] = kmsKeyName,
      customerEncryption: Option[CustomerEncryption] = customerEncryption,
      owner: Option[Owner] = owner,
      acl: Option[List[ObjectAccessControls]] = acl
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
    timeDeleted = Some(timeCreated),
    storageClass = storageClass,
    contentDisposition = contentDisposition,
    contentEncoding = contentEncoding,
    contentLanguage = contentLanguage,
    metageneration = metageneration,
    temporaryHold = temporaryHold,
    eventBasedHold = eventBasedHold,
    retentionExpirationTime = retentionExpirationTime,
    timeStorageClassUpdated = timeStorageClassUpdated,
    cacheControl = cacheControl,
    metadata = metadata,
    componentCount = componentCount,
    kmsKeyName = kmsKeyName,
    customerEncryption = customerEncryption,
    owner = owner,
    acl = acl
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
    timeDeleted.fold("")(td => s"timeDeleted=$td,") +
    s"storageClass=$storageClass," +
    s"contentDisposition=$contentDisposition," +
    s"contentEncoding=$contentEncoding," +
    s"contentLanguage=$contentLanguage" +
    s"metageneration = $metageneration," +
    s"temporaryHold = $temporaryHold," +
    s"eventBasedHold = $eventBasedHold," +
    s"retentionExpirationTime = $retentionExpirationTime," +
    s"timeStorageClassUpdated = $timeStorageClassUpdated," +
    s"cacheControl = $cacheControl," +
    metadata.fold("")(m => s"metadata = $m,") +
    componentCount.fold("")(cc => s"componentCount = $cc,") +
    kmsKeyName.fold("")(kkn => s"kmsKeyName = $kkn,") +
    customerEncryption.fold("")(ce => s"customerEncryption = $ce,") +
    owner.fold("")(o => s"owner = $o,") +
    acl.fold("")(acls => acls.mkString("[", ",", "]")) +
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
      java.util.Objects.equals(this.timeDeleted, that.timeDeleted) &&
      java.util.Objects.equals(this.storageClass, that.storageClass) &&
      java.util.Objects.equals(this.contentDisposition, that.contentDisposition) &&
      java.util.Objects.equals(this.contentEncoding, that.contentEncoding) &&
      java.util.Objects.equals(this.contentLanguage, that.contentLanguage) &&
      java.util.Objects.equals(this.metageneration, that.metageneration) &&
      java.util.Objects.equals(this.temporaryHold, that.temporaryHold) &&
      java.util.Objects.equals(this.eventBasedHold, that.eventBasedHold) &&
      java.util.Objects.equals(this.retentionExpirationTime, that.retentionExpirationTime) &&
      java.util.Objects.equals(this.timeStorageClassUpdated, that.timeStorageClassUpdated) &&
      java.util.Objects.equals(this.cacheControl, that.cacheControl) &&
      java.util.Objects.equals(this.metadata, that.metadata) &&
      java.util.Objects.equals(this.componentCount, that.componentCount) &&
      java.util.Objects.equals(this.kmsKeyName, that.kmsKeyName)
      java.util.Objects.equals(this.customerEncryption, that.customerEncryption)
      java.util.Objects.equals(this.owner, that.owner)
      java.util.Objects.equals(this.acl, that.acl)
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
      updated,
      timeCreated,
      timeDeleted,
      storageClass,
      contentDisposition,
      contentEncoding,
      contentLanguage,
      Long.box(metageneration),
      temporaryHold.map(Boolean.box),
      eventBasedHold.map(Boolean.box),
      retentionExpirationTime,
      timeStorageClassUpdated,
      cacheControl,
      metadata,
      componentCount.map(Int.box),
      kmsKeyName,
      customerEncryption,
      owner,
      acl
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
      updated: OffsetDateTime,
      timeCreated: OffsetDateTime,
      timeDeleted: Option[OffsetDateTime],
      storageClass: String,
      contentDisposition: Option[String],
      contentEncoding: Option[String],
      contentLanguage: Option[String],
      metageneration: Long,
      temporaryHold: Option[Boolean],
      eventBasedHold: Option[Boolean],
      retentionExpirationTime: Option[OffsetDateTime],
      timeStorageClassUpdated: OffsetDateTime,
      cacheControl: Option[String],
      metadata: Option[Map[String, String]],
      componentCount: Option[Int],
      kmsKeyName: Option[String],
      customerEncryption: Option[CustomerEncryption],
      owner: Option[Owner],
      acl: Option[List[ObjectAccessControls]]
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
    timeDeleted,
    storageClass,
    contentDisposition,
    contentEncoding,
    contentLanguage,
    metageneration,
    temporaryHold,
    eventBasedHold,
    retentionExpirationTime,
    timeStorageClassUpdated,
    cacheControl,
    metadata,
    componentCount,
    kmsKeyName,
    customerEncryption,
    owner,
    acl
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
      updated: OffsetDateTime,
      timeCreated: OffsetDateTime,
      timeDeleted: Optional[OffsetDateTime],
      storageClass: String,
      contentDisposition: Optional[String],
      contentEncoding: Optional[String],
      contentLanguage: Optional[String],
      metageneration: Long,
      temporaryHold: Optional[Boolean],
      eventBasedHold: Optional[Boolean],
      retentionExpirationTime: Optional[OffsetDateTime],
      timeStorageClassUpdated: OffsetDateTime,
      cacheControl: Optional[String],
      metadata: Optional[Map[String, String]],
      componentCount: Optional[Int],
      kmsKeyName: Optional[String],
      customerEncryption: Optional[CustomerEncryption],
      owner: Optional[Owner],
      acl: Optional[List[ObjectAccessControls]]
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
    timeDeleted.asScala,
    storageClass,
    contentDisposition.asScala,
    contentEncoding.asScala,
    contentLanguage.asScala,
    metageneration,
    temporaryHold.asScala,
    eventBasedHold.asScala,
    retentionExpirationTime.asScala,
    timeStorageClassUpdated,
    cacheControl.asScala,
    metadata.asScala,
    componentCount.asScala,
    kmsKeyName.asScala,
    customerEncryption.asScala,
    owner.asScala,
    acl.asScala
  )
}
