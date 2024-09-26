/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.s3

import java.time.Instant
import java.util.{Objects, Optional}
import akka.http.scaladsl.model.{DateTime, HttpHeader, IllegalUriException, Uri}
import akka.http.scaladsl.model.headers._
import akka.stream.alpakka.s3.AccessStyle.PathAccessStyle

import scala.annotation.nowarn
import scala.collection.immutable.Seq
import scala.collection.immutable
import scala.jdk.CollectionConverters._
import scala.jdk.OptionConverters._

final class MultipartUpload private (val bucket: String, val key: String, val uploadId: String) {

  /** Java API */
  def getBucket: String = bucket

  /** Java API */
  def getKey: String = key

  /** Java API */
  def getUploadId: String = uploadId

  def withBucket(value: String): MultipartUpload = copy(bucket = value)

  def withKey(value: String): MultipartUpload = copy(key = value)

  def withUploadId(value: String): MultipartUpload = copy(uploadId = value)

  private def copy(bucket: String = bucket, key: String = key, uploadId: String = uploadId): MultipartUpload =
    new MultipartUpload(
      bucket,
      key,
      uploadId
    )

  override def toString: String =
    "MultipartUpload(" +
    s"bucket=$bucket," +
    s"key=$key," +
    s"uploadId=$uploadId" +
    ")"

  override def equals(other: Any): Boolean = other match {
    case that: MultipartUpload =>
      Objects.equals(this.bucket, that.bucket) &&
      Objects.equals(this.key, that.key) &&
      Objects.equals(this.uploadId, that.uploadId)
    case _ => false
  }

  override def hashCode(): Int =
    Objects.hash(bucket, key, uploadId)
}

object MultipartUpload {

  /** Scala API */
  def apply(bucket: String, key: String, uploadId: String): MultipartUpload = {
    new MultipartUpload(bucket, key, uploadId)
  }

  /** Java API */
  def create(bucket: String, key: String, uploadId: String): MultipartUpload = apply(bucket, key, uploadId)
}

sealed trait UploadPartResponse {

  /** Scala API */
  def multipartUpload: MultipartUpload

  /** Scala API */
  def partNumber: Int

  /** Java API */
  def getMultipartUpload: MultipartUpload = multipartUpload

  /** Java API */
  def getPartNumber: Int = partNumber
}

final class SuccessfulUploadPart private (val multipartUpload: MultipartUpload, val partNumber: Int, val eTag: String)
    extends UploadPartResponse {

  /** Java API */
  def getETag: String = eTag

  def withMultipartUpload(value: MultipartUpload): SuccessfulUploadPart = copy(multipartUpload = value)

  def withPartNumber(value: Int): SuccessfulUploadPart = copy(partNumber = value)

  def withETag(value: String): SuccessfulUploadPart = copy(eTag = value)

  private def copy(multipartUpload: MultipartUpload = multipartUpload,
                   partNumber: Int = partNumber,
                   eTag: String = eTag): SuccessfulUploadPart =
    new SuccessfulUploadPart(multipartUpload, partNumber, eTag)

  override def toString: String =
    "SuccessfulUploadPart(" +
    s"multipartUpload=$multipartUpload," +
    s"partNumber=$partNumber," +
    s"eTag=$eTag" +
    ")"

  override def equals(other: Any): Boolean = other match {
    case that: SuccessfulUploadPart =>
      Objects.equals(this.multipartUpload, that.multipartUpload) &&
      Objects.equals(this.partNumber, that.partNumber) &&
      Objects.equals(this.eTag, that.eTag)
    case _ => false
  }

  override def hashCode(): Int =
    Objects.hash(multipartUpload, Int.box(partNumber), eTag)

}

object SuccessfulUploadPart {

  /** Scala API */
  def apply(multipartUpload: MultipartUpload, partNumber: Int, eTag: String): SuccessfulUploadPart =
    new SuccessfulUploadPart(
      multipartUpload,
      partNumber,
      eTag
    )

  /** Java API */
  def create(multipartUpload: MultipartUpload, partNumber: Int, eTag: String): SuccessfulUploadPart =
    apply(multipartUpload, partNumber, eTag)
}

final class FailedUploadPart private (val multipartUpload: MultipartUpload,
                                      val partNumber: Int,
                                      val exception: Throwable)
    extends UploadPartResponse {

  /** Java API */
  def getException: Throwable = exception

  def withMultipartUpload(value: MultipartUpload): FailedUploadPart = copy(multipartUpload = value)

  def withPartNumber(value: Int): FailedUploadPart = copy(partNumber = value)

  def withException(value: Throwable): FailedUploadPart = copy(exception = value)

  private def copy(multipartUpload: MultipartUpload = multipartUpload,
                   partNumber: Int = partNumber,
                   exception: Throwable = exception): FailedUploadPart =
    new FailedUploadPart(multipartUpload, partNumber, exception)

  override def toString: String =
    "FailedUploadPart(" +
    s"multipartUpload=$multipartUpload," +
    s"partNumber=$partNumber," +
    s"exception=$exception" +
    ")"

  override def equals(other: Any): Boolean =
    other match {
      case that: FailedUploadPart =>
        Objects.equals(this.multipartUpload, that.multipartUpload) &&
        Objects.equals(this.partNumber, that.partNumber) &&
        Objects.equals(this.exception, that.exception)
      case _ => false
    }

  override def hashCode(): Int =
    Objects.hash(multipartUpload, Int.box(partNumber), exception)
}

object FailedUploadPart {

  /** Scala API */
  def apply(multipartUpload: MultipartUpload, partNumber: Int, exception: Throwable): FailedUploadPart =
    new FailedUploadPart(multipartUpload, partNumber, exception)

  /** Java API */
  def create(multipartUpload: MultipartUpload, partNumber: Int, exception: Throwable): FailedUploadPart =
    apply(multipartUpload, partNumber, exception)
}

final class MultipartUploadResult private (
    val location: Uri,
    val bucket: String,
    val key: String,
    val eTag: String,
    val versionId: Option[String]
) {

  /** Java API */
  def getLocation: akka.http.javadsl.model.Uri = akka.http.javadsl.model.Uri.create(location)

  /** Java API */
  def getBucket: String = bucket

  /** Java API */
  def getKey: String = key

  /** Java API */
  def getETag: String = eTag

  /** Java API */
  def getVersionId: Optional[String] = versionId.toJava

  def withLocation(value: Uri): MultipartUploadResult = copy(location = value)
  def withBucket(value: String): MultipartUploadResult = copy(bucket = value)
  def withKey(value: String): MultipartUploadResult = copy(key = value)
  def withETag(value: String): MultipartUploadResult = copy(eTag = value)
  def withVersionId(value: String): MultipartUploadResult =
    // See https://docs.aws.amazon.com/AmazonS3/latest/userguide/AddingObjectstoVersionSuspendedBuckets.html for more
    // info.
    if (value.trim.toLowerCase == "null")
      copy(versionId = None)
    else
      copy(versionId = Option(value))

  private def copy(
      location: Uri = location,
      bucket: String = bucket,
      key: String = key,
      eTag: String = eTag,
      versionId: Option[String] = versionId
  ): MultipartUploadResult = new MultipartUploadResult(
    location = location,
    bucket = bucket,
    key = key,
    eTag = eTag,
    versionId = versionId
  )

  override def toString: String =
    "MultipartUploadResult(" +
    s"location=$location," +
    s"bucket=$bucket," +
    s"key=$key," +
    s"eTag=$eTag," +
    s"versionId=$versionId" +
    ")"

  override def equals(other: Any): Boolean = other match {
    case that: MultipartUploadResult =>
      Objects.equals(this.location, that.location) &&
      Objects.equals(this.bucket, that.bucket) &&
      Objects.equals(this.key, that.key) &&
      Objects.equals(this.eTag, that.eTag) &&
      Objects.equals(this.versionId, that.versionId)
    case _ => false
  }

  override def hashCode(): Int =
    Objects.hash(location, bucket, key, eTag, versionId)
}

object MultipartUploadResult {

  /** Scala API */
  def apply(
      location: Uri,
      bucket: String,
      key: String,
      eTag: String,
      versionId: Option[String]
  ): MultipartUploadResult = {
    // See https://docs.aws.amazon.com/AmazonS3/latest/userguide/AddingObjectstoVersionSuspendedBuckets.html for more
    // info.
    val finalVersionId = versionId match {
      case Some(s) if s.trim.toLowerCase == "null" => None
      case rest => rest
    }

    new MultipartUploadResult(
      location,
      bucket,
      key,
      eTag,
      finalVersionId
    )
  }

  /** Java API */
  def create(
      location: akka.http.javadsl.model.Uri,
      bucket: String,
      key: String,
      eTag: String,
      versionId: java.util.Optional[String]
  ): MultipartUploadResult = apply(
    location.asScala(),
    bucket,
    key,
    eTag,
    versionId.toScala
  )
}

final class AWSIdentity private (val id: String, val displayName: String) {

  /** Java API */
  def getId: String = id

  /** Java API */
  def getDisplayName: String = displayName

  def withId(value: String): AWSIdentity = copy(id = value)
  def withDisplayName(value: String): AWSIdentity = copy(displayName = value)

  private def copy(id: String = id, displayName: String = displayName): AWSIdentity = new AWSIdentity(
    id,
    displayName
  )

  override def toString: String =
    "AWSIdentity(" +
    s"id=$id," +
    s"displayName=$displayName" +
    ")"

  override def equals(other: Any): Boolean =
    other match {
      case that: AWSIdentity =>
        Objects.equals(this.id, that.id) &&
        Objects.equals(this.displayName, that.displayName)
      case _ => false
    }

  override def hashCode(): Int =
    Objects.hash(id, displayName)

}

object AWSIdentity {

  /** Scala API */
  def apply(id: String, displayName: String): AWSIdentity = new AWSIdentity(id, displayName)

  /** Java API */
  def create(id: String, displayName: String): AWSIdentity = apply(id, displayName)

}

final class ListMultipartUploadResultUploads private (val key: String,
                                                      val uploadId: String,
                                                      val initiator: Option[AWSIdentity],
                                                      val owner: Option[AWSIdentity],
                                                      val storageClass: String,
                                                      val initiated: Instant) {

  /** Java API */
  def getKey: String = key

  /** Java API */
  def getUploadId: String = uploadId

  /** Java API */
  def getInitiator: Optional[AWSIdentity] = initiator.toJava

  /** Java API */
  def getOwner: Optional[AWSIdentity] = owner.toJava

  /** Java API */
  def getStorageClass: String = storageClass

  /** Java API */
  def getInitiated: Instant = initiated

  def withKey(value: String): ListMultipartUploadResultUploads = copy(key = value)
  def withUploadId(value: String): ListMultipartUploadResultUploads = copy(uploadId = value)
  def withInitiator(value: AWSIdentity): ListMultipartUploadResultUploads = copy(initiator = Option(value))
  def withOwner(value: AWSIdentity): ListMultipartUploadResultUploads = copy(owner = Option(value))
  def withStorageClass(value: String): ListMultipartUploadResultUploads = copy(storageClass = value)
  def withInitiated(value: Instant): ListMultipartUploadResultUploads = copy(initiated = value)

  private def copy(key: String = key,
                   uploadId: String = uploadId,
                   initiator: Option[AWSIdentity] = initiator,
                   owner: Option[AWSIdentity] = owner,
                   storageClass: String = storageClass,
                   initiated: Instant = initiated): ListMultipartUploadResultUploads =
    new ListMultipartUploadResultUploads(
      key = key,
      uploadId = uploadId,
      initiator = initiator,
      owner = owner,
      storageClass = storageClass,
      initiated = initiated
    )

  override def toString: String =
    "ListMultipartUploadResultUploads(" +
    s"key=$key," +
    s"uploadId=$uploadId," +
    s"initiator=$initiator," +
    s"owner=$owner," +
    s"storageClass=$storageClass," +
    s"initiated=$initiated" +
    ")"

  override def equals(other: Any): Boolean =
    other match {
      case that: ListMultipartUploadResultUploads =>
        Objects.equals(this.key, that.key) &&
        Objects.equals(this.uploadId, that.uploadId) &&
        Objects.equals(this.initiator, that.initiator) &&
        Objects.equals(this.owner, that.owner) &&
        Objects.equals(this.storageClass, that.storageClass) &&
        Objects.equals(this.initiated, that.initiated)
      case _ => false
    }

  override def hashCode(): Int =
    Objects.hash(key, uploadId, initiator, owner, storageClass, initiated)
}

object ListMultipartUploadResultUploads {

  /** Scala API */
  def apply(key: String,
            uploadId: String,
            initiator: Option[AWSIdentity],
            owner: Option[AWSIdentity],
            storageClass: String,
            initiated: Instant): ListMultipartUploadResultUploads =
    new ListMultipartUploadResultUploads(key, uploadId, initiator, owner, storageClass, initiated)

  /** Java API */
  def create(key: String,
             uploadId: String,
             initiator: Optional[AWSIdentity],
             owner: Optional[AWSIdentity],
             storageClass: String,
             initiated: Instant): ListMultipartUploadResultUploads =
    apply(key, uploadId, initiator.toScala, owner.toScala, storageClass, initiated)
}

final class ListObjectVersionsResultVersions private (val eTag: String,
                                                      val isLatest: Boolean,
                                                      val key: String,
                                                      val lastModified: Instant,
                                                      val owner: Option[AWSIdentity],
                                                      val size: Long,
                                                      val storageClass: String,
                                                      val versionId: Option[String]) {

  /** Java API */
  def getETag: String = eTag

  /** Java API */
  def getIsLatest: Boolean = isLatest

  /** Java API */
  def getKey: String = key

  /** Java API */
  def getLastModified: Instant = lastModified

  /** Java API */
  def getOwner: Optional[AWSIdentity] = owner.toJava

  /** Java API */
  def getSize: Long = size

  /** Java API */
  def getStorageClass: String = storageClass

  /** Java API */
  def getVersionId: Optional[String] = versionId.toJava

  def withETag(value: String): ListObjectVersionsResultVersions = copy(eTag = value)

  def withIsLatest(value: Boolean): ListObjectVersionsResultVersions = copy(isLatest = value)

  def withKey(value: String): ListObjectVersionsResultVersions = copy(key = value)

  def withLastModified(value: Instant): ListObjectVersionsResultVersions = copy(lastModified = value)

  def withOwner(value: AWSIdentity): ListObjectVersionsResultVersions = copy(owner = Option(value))

  def withSize(value: Long): ListObjectVersionsResultVersions = copy(size = value)

  def withStorageClass(value: String): ListObjectVersionsResultVersions = copy(storageClass = value)

  def withVersionId(value: String): ListObjectVersionsResultVersions =
    // See https://docs.aws.amazon.com/AmazonS3/latest/userguide/AddingObjectstoVersionSuspendedBuckets.html for more
    // info.
    if (value.trim.toLowerCase == "null")
      copy(versionId = None)
    else
      copy(versionId = Option(value))

  private def copy(eTag: String = eTag,
                   isLatest: Boolean = isLatest,
                   key: String = key,
                   lastModified: Instant = lastModified,
                   owner: Option[AWSIdentity] = owner,
                   size: Long = size,
                   storageClass: String = storageClass,
                   versionId: Option[String] = versionId): ListObjectVersionsResultVersions =
    new ListObjectVersionsResultVersions(
      eTag = eTag,
      isLatest = isLatest,
      key = key,
      lastModified = lastModified,
      owner = owner,
      size = size,
      storageClass = storageClass,
      versionId = versionId
    )

  override def toString: String =
    "ListObjectVersionsResultVersions(" +
    s"eTag=$eTag," +
    s"isLatest=$isLatest," +
    s"key=$key," +
    s"lastModified=$lastModified," +
    s"owner=$owner," +
    s"size=$size," +
    s"storageClass=$storageClass," +
    s"versionId=$versionId" +
    ")"

  override def equals(other: Any): Boolean =
    other match {
      case that: ListObjectVersionsResultVersions =>
        Objects.equals(this.eTag, that.eTag) &&
        Objects.equals(this.isLatest, that.isLatest) &&
        Objects.equals(this.key, that.key) &&
        Objects.equals(this.lastModified, that.lastModified) &&
        Objects.equals(this.owner, that.owner) &&
        Objects.equals(this.size, that.size) &&
        Objects.equals(this.storageClass, that.storageClass) &&
        Objects.equals(this.versionId, that.versionId)
      case _ => false
    }

  override def hashCode(): Int =
    Objects.hash(eTag, Boolean.box(isLatest), key, lastModified, owner, Long.box(size), storageClass, versionId)
}

object ListObjectVersionsResultVersions {

  /** Scala API */
  def apply(eTag: String,
            isLatest: Boolean,
            key: String,
            lastModified: Instant,
            owner: Option[AWSIdentity],
            size: Long,
            storageClass: String,
            versionId: Option[String]): ListObjectVersionsResultVersions = {
    // See https://docs.aws.amazon.com/AmazonS3/latest/userguide/AddingObjectstoVersionSuspendedBuckets.html for more
    // info.
    val finalVersionId = versionId match {
      case Some(s) if s.trim.toLowerCase == "null" => None
      case rest => rest
    }

    new ListObjectVersionsResultVersions(eTag, isLatest, key, lastModified, owner, size, storageClass, finalVersionId)
  }

  /** Java API */
  def create(eTag: String,
             isLatest: Boolean,
             key: String,
             lastModified: Instant,
             owner: Optional[AWSIdentity],
             size: Long,
             storageClass: String,
             versionId: Optional[String]): ListObjectVersionsResultVersions =
    apply(eTag, isLatest, key, lastModified, owner.toScala, size, storageClass, versionId.toScala)
}

final class DeleteMarkers private (val isLatest: Boolean,
                                   val key: String,
                                   val lastModified: Instant,
                                   val owner: Option[AWSIdentity],
                                   val versionId: Option[String]) {

  /** Java API */
  def getIsLatest: Boolean = isLatest

  /** Java API */
  def getKey: String = key

  /** Java API */
  def getLastModified: Instant = lastModified

  /** Java API */
  def getOwner: Optional[AWSIdentity] = owner.toJava

  /** Java API */
  def getVersionId: Optional[String] = versionId.toJava

  def withIsLatest(value: Boolean): DeleteMarkers = copy(isLatest = value)

  def withKey(value: String): DeleteMarkers = copy(key = value)

  def withLastModified(value: Instant): DeleteMarkers = copy(lastModified = value)

  def withOwner(value: AWSIdentity): DeleteMarkers = copy(owner = Option(value))

  def withVersionId(value: String): DeleteMarkers =
    // See https://docs.aws.amazon.com/AmazonS3/latest/userguide/AddingObjectstoVersionSuspendedBuckets.html for more
    // info.
    if (value.trim.toLowerCase == "null")
      copy(versionId = None)
    else
      copy(versionId = Option(value))

  private def copy(isLatest: Boolean = isLatest,
                   key: String = key,
                   lastModified: Instant = lastModified,
                   owner: Option[AWSIdentity] = owner,
                   versionId: Option[String] = versionId): DeleteMarkers =
    new DeleteMarkers(isLatest, key, lastModified, owner, versionId)

  override def toString: String =
    "DeleteMarkers(" +
    s"isLatest=$isLatest," +
    s"key=$key," +
    s"lastModified=$lastModified," +
    s"owner=$owner," +
    s"versionId=$versionId" +
    ")"

  override def equals(other: Any): Boolean =
    other match {
      case that: DeleteMarkers =>
        Objects.equals(this.isLatest, that.isLatest) &&
        Objects.equals(this.key, that.key) &&
        Objects.equals(this.lastModified, that.lastModified) &&
        Objects.equals(this.owner, that.owner) &&
        Objects.equals(this.versionId, that.versionId)
      case _ => false
    }

  override def hashCode(): Int =
    Objects.hash(Boolean.box(isLatest), key, lastModified, owner, owner, versionId)
}

object DeleteMarkers {

  /** Scala API */
  def apply(isLatest: Boolean,
            key: String,
            lastModified: Instant,
            owner: Option[AWSIdentity],
            versionId: Option[String]): DeleteMarkers = {
    // See https://docs.aws.amazon.com/AmazonS3/latest/userguide/AddingObjectstoVersionSuspendedBuckets.html for more
    // info.
    val finalVersionId = versionId match {
      case Some(s) if s.trim.toLowerCase == "null" => None
      case rest => rest
    }

    new DeleteMarkers(isLatest, key, lastModified, owner, finalVersionId)
  }

  /** Java API */
  def create(isLatest: Boolean,
             key: String,
             lastModified: Instant,
             owner: Optional[AWSIdentity],
             versionId: Optional[String]): DeleteMarkers =
    apply(isLatest, key, lastModified, owner.toScala, versionId.toScala)
}

final class CommonPrefixes private (val prefix: String) {

  /** Java API */
  def getPrefix: String = prefix

  def withPrefix(value: String): CommonPrefixes = copy(prefix = value)

  // Warning is only being generated here because there is a single argument in the parameter list. If more fields
  // get added to CommonPrefixes then the `@nowarn` is no longer needed
  @nowarn
  private def copy(prefix: String = prefix): CommonPrefixes =
    new CommonPrefixes(prefix)

  override def toString: String =
    "CommonPrefixes(" +
    s"prefix=$prefix" +
    ")"

  override def equals(other: Any): Boolean =
    other match {
      case that: CommonPrefixes =>
        Objects.equals(this.prefix, that.prefix)
      case _ => false
    }

  override def hashCode(): Int =
    Objects.hash(prefix)
}

object CommonPrefixes {

  /** Scala API */
  def apply(prefix: String): CommonPrefixes =
    new CommonPrefixes(prefix)

  /** Java API */
  def create(prefix: String): CommonPrefixes = apply(prefix)
}

final class ListPartsResultParts(val lastModified: Instant, val eTag: String, val partNumber: Int, val size: Long) {

  /** Java API */
  def getLastModified: Instant = lastModified

  /** Java API */
  def getETag: String = eTag

  /** Java API */
  def getPartNumber: Int = partNumber

  /** Java API */
  def getSize: Long = size

  def withLastModified(value: Instant): ListPartsResultParts = copy(lastModified = value)
  def withETag(value: String): ListPartsResultParts = copy(eTag = value)
  def withPartNumber(value: Int): ListPartsResultParts = copy(partNumber = value)
  def withSize(value: Long): ListPartsResultParts = copy(size = value)

  private def copy(lastModified: Instant = lastModified,
                   eTag: String = eTag,
                   partNumber: Int = partNumber,
                   size: Long = size): ListPartsResultParts =
    new ListPartsResultParts(
      lastModified,
      eTag,
      partNumber,
      size
    )

  override def toString: String =
    "ListPartsResultParts(" +
    s"lastModified=$lastModified," +
    s"eTag=$eTag," +
    s"partNumber=$partNumber," +
    s"size=$size" +
    ")"

  override def equals(other: Any): Boolean =
    other match {
      case that: ListPartsResultParts =>
        Objects.equals(this.lastModified, that.lastModified) &&
        Objects.equals(this.eTag, that.eTag) &&
        Objects.equals(this.partNumber, that.partNumber) &&
        Objects.equals(this.size, that.size)
      case _ => false
    }

  override def hashCode(): Int =
    Objects.hash(lastModified, eTag, Int.box(partNumber), Long.box(size))

  def toPart: Part = Part(eTag, partNumber)
}

object ListPartsResultParts {

  /** Scala API */
  def apply(lastModified: Instant, eTag: String, partNumber: Int, size: Long): ListPartsResultParts =
    new ListPartsResultParts(lastModified, eTag, partNumber, size)

  /** Java API */
  def create(lastModified: Instant, eTag: String, partNumber: Int, size: Long): ListPartsResultParts =
    apply(lastModified, eTag, partNumber, size)
}

final class Part(val eTag: String, val partNumber: Int) {

  /** Java API */
  def getETag: String = eTag

  /** Java API */
  def getPartNumber: Int = partNumber

  def withETag(value: String): Part = copy(eTag = value)

  def withPartNumber(value: Int): Part = copy(partNumber = value)

  private def copy(eTag: String = eTag, partNumber: Int = partNumber): Part = new Part(eTag, partNumber)

  override def toString: String =
    "Part(" +
    s"eTag=$eTag," +
    s"partNumber=$partNumber" +
    ")"

  override def equals(other: Any): Boolean =
    other match {
      case that: Part =>
        Objects.equals(this.eTag, that.eTag) &&
        Objects.equals(this.partNumber, that.partNumber)
      case _ => false
    }

  override def hashCode(): Int =
    Objects.hash(this.eTag, Int.box(this.partNumber))

}

object Part {

  /** Scala API */
  def apply(eTag: String, partNumber: Int): Part = new Part(eTag, partNumber)

  /** Java API */
  def create(eTag: String, partNumber: Int): Part = new Part(eTag, partNumber)
}

/**
 * Thrown when multipart upload or multipart copy fails because of a server failure.
 */
final class FailedUpload private (
    val reasons: Seq[Throwable]
) extends Exception(reasons.map(_.getMessage).mkString(", ")) {

  /** Java API */
  def getReasons: java.util.List[Throwable] = reasons.asJava
}

object FailedUpload {

  def apply(reasons: Seq[Throwable]): FailedUpload = new FailedUpload(reasons)

  /** Java API */
  def create(reasons: Seq[Throwable]): FailedUpload = FailedUpload(reasons)
}

final class ListBucketsResultContents private (val creationDate: java.time.Instant, val name: String) {

  /** Java API */
  def getCreationDate: java.time.Instant = creationDate

  /** Java API */
  def getName: String = name

  def withCreationDate(value: java.time.Instant): ListBucketsResultContents = copy(creationDate = value)

  def withName(value: String): ListBucketsResultContents = copy(name = value)

  private def copy(
      name: String = name,
      creationDate: java.time.Instant = creationDate
  ): ListBucketsResultContents = new ListBucketsResultContents(
    name = name,
    creationDate = creationDate
  )

  override def toString: String =
    "ListBucketsResultContents(" +
    s"creationDate=$creationDate," +
    s"name=$name" +
    ")"

  override def equals(other: Any): Boolean = other match {
    case that: ListBucketsResultContents =>
      Objects.equals(this.name, that.name) &&
      Objects.equals(this.creationDate, that.creationDate)
    case _ => false
  }

  override def hashCode(): Int =
    Objects.hash(name, creationDate)
}

object ListBucketsResultContents {

  /** Scala API */
  def apply(
      creationDate: java.time.Instant,
      name: String
  ): ListBucketsResultContents = new ListBucketsResultContents(
    creationDate,
    name
  )

  /** Java API */
  def create(
      creationDate: java.time.Instant,
      name: String
  ): ListBucketsResultContents = apply(
    creationDate,
    name
  )
}

/**
 * @param bucketName The name of the bucket in which this object is stored
 * @param key The key under which this object is stored
 * @param eTag Hex encoded MD5 hash of this object's contents, as computed by Amazon S3
 * @param size The size of this object, in bytes
 * @param lastModified The date, according to Amazon S3, when this object was last modified
 * @param storageClass The class of storage used by Amazon S3 to store this object
 */
final class ListBucketResultContents private (
    val bucketName: String,
    val key: String,
    val eTag: String,
    val size: Long,
    val lastModified: java.time.Instant,
    val storageClass: String
) {

  /** Java API */
  def getBucketName: String = bucketName

  /** Java API */
  def getKey: String = key

  /** Java API */
  def getETag: String = eTag

  /** Java API */
  def getSize: Long = size

  /** Java API */
  def getLastModified: java.time.Instant = lastModified

  /** Java API */
  def getStorageClass: String = storageClass

  def withBucketName(value: String): ListBucketResultContents = copy(bucketName = value)
  def withKey(value: String): ListBucketResultContents = copy(key = value)
  def withETag(value: String): ListBucketResultContents = copy(eTag = value)
  def withSize(value: Long): ListBucketResultContents = copy(size = value)
  def withLastModified(value: java.time.Instant): ListBucketResultContents = copy(lastModified = value)
  def withStorageClass(value: String): ListBucketResultContents = copy(storageClass = value)

  private def copy(
      bucketName: String = bucketName,
      key: String = key,
      eTag: String = eTag,
      size: Long = size,
      lastModified: java.time.Instant = lastModified,
      storageClass: String = storageClass
  ): ListBucketResultContents = new ListBucketResultContents(
    bucketName = bucketName,
    key = key,
    eTag = eTag,
    size = size,
    lastModified = lastModified,
    storageClass = storageClass
  )

  override def toString: String =
    "ListBucketResultContents(" +
    s"bucketName=$bucketName," +
    s"key=$key," +
    s"eTag=$eTag," +
    s"size=$size," +
    s"lastModified=$lastModified," +
    s"storageClass=$storageClass" +
    ")"

  override def equals(other: Any): Boolean = other match {
    case that: ListBucketResultContents =>
      Objects.equals(this.bucketName, that.bucketName) &&
      Objects.equals(this.key, that.key) &&
      Objects.equals(this.eTag, that.eTag) &&
      Objects.equals(this.size, that.size) &&
      Objects.equals(this.lastModified, that.lastModified) &&
      Objects.equals(this.storageClass, that.storageClass)
    case _ => false
  }

  override def hashCode(): Int =
    Objects.hash(bucketName, key, eTag, Long.box(size), lastModified, storageClass)
}

object ListBucketResultContents {

  /** Scala API */
  def apply(
      bucketName: String,
      key: String,
      eTag: String,
      size: Long,
      lastModified: java.time.Instant,
      storageClass: String
  ): ListBucketResultContents = new ListBucketResultContents(
    bucketName,
    key,
    eTag,
    size,
    lastModified,
    storageClass
  )

  /** Java API */
  def create(
      bucketName: String,
      key: String,
      eTag: String,
      size: Long,
      lastModified: java.time.Instant,
      storageClass: String
  ): ListBucketResultContents = apply(
    bucketName,
    key,
    eTag,
    size,
    lastModified,
    storageClass
  )
}

/**
 * @param bucketName The name of the bucket in which this object is stored
 * @param prefix The common prefix of keys between Prefix and the next occurrence of the string specified by a delimiter.
 */
final class ListBucketResultCommonPrefixes private (
    val bucketName: String,
    val prefix: String
) {

  /** Java API */
  def getBucketName: String = bucketName

  /** Java API */
  def getPrefix: String = prefix

  def withBucketName(value: String): ListBucketResultCommonPrefixes = copy(bucketName = value)
  def withPrefix(value: String): ListBucketResultCommonPrefixes = copy(prefix = value)

  private def copy(
      bucketName: String = bucketName,
      prefix: String = prefix
  ): ListBucketResultCommonPrefixes = new ListBucketResultCommonPrefixes(
    bucketName = bucketName,
    prefix = prefix
  )

  override def toString: String =
    "ListBucketResultCommonPrefixes(" +
    s"bucketName=$bucketName," +
    s"prefix=$prefix" +
    ")"

  override def equals(other: Any): Boolean = other match {
    case that: ListBucketResultCommonPrefixes =>
      Objects.equals(this.bucketName, that.bucketName) &&
      Objects.equals(this.prefix, that.prefix)
    case _ => false
  }

  override def hashCode(): Int =
    Objects.hash(bucketName, prefix)
}

object ListBucketResultCommonPrefixes {

  /** Scala API */
  def apply(
      bucketName: String,
      prefix: String
  ): ListBucketResultCommonPrefixes = new ListBucketResultCommonPrefixes(
    bucketName,
    prefix
  )

  /** Java API */
  def create(
      bucketName: String,
      prefix: String
  ): ListBucketResultCommonPrefixes = apply(
    bucketName,
    prefix
  )
}

/**
 * Modelled after com.amazonaws.services.s3.model.ObjectMetadata
 *
 * @param metadata the raw http headers
 */
final class ObjectMetadata private (
    val metadata: Seq[HttpHeader]
) {

  /**
   * Java Api
   */
  lazy val headers: java.util.List[akka.http.javadsl.model.HttpHeader] =
    (metadata: immutable.Seq[akka.http.javadsl.model.HttpHeader]).asJava

  /**
   * Gets the hex encoded 128-bit MD5 digest of the associated object
   * according to RFC 1864. This data is used as an integrity check to verify
   * that the data received by the caller is the same data that was sent by
   * Amazon S3.
   * <p>
   * This field represents the hex encoded 128-bit MD5 digest of an object's
   * content as calculated by Amazon S3. The ContentMD5 field represents the
   * base64 encoded 128-bit MD5 digest as calculated on the caller's side.
   * </p>
   *
   * @return The hex encoded MD5 hash of the content for the associated object
   *         as calculated by Amazon S3.
   */
  lazy val eTag: Option[String] = metadata.collectFirst {
    case e: ETag => Utils.removeQuotes(e.etag.value)
  }

  /**
   * Java Api
   *
   * Gets the hex encoded 128-bit MD5 digest of the associated object
   * according to RFC 1864. This data is used as an integrity check to verify
   * that the data received by the caller is the same data that was sent by
   * Amazon S3.
   * <p>
   * This field represents the hex encoded 128-bit MD5 digest of an object's
   * content as calculated by Amazon S3. The ContentMD5 field represents the
   * base64 encoded 128-bit MD5 digest as calculated on the caller's side.
   * </p>
   *
   * @return The hex encoded MD5 hash of the content for the associated object
   *         as calculated by Amazon S3.
   */
  lazy val getETag: Optional[String] =
    eTag.toJava

  /**
   * <p>
   * Gets the Content-Length HTTP header indicating the size of the
   * associated object in bytes.
   * </p>
   * <p>
   * This field is required when uploading objects to S3, but the AWS S3 Java
   * client will automatically set it when working directly with files. When
   * uploading directly from a stream, set this field if
   * possible. Otherwise the client must buffer the entire stream in
   * order to calculate the content length before sending the data to
   * Amazon S3.
   * </p>
   * <p>
   * For more information on the Content-Length HTTP header, see [[https://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html#sec14.13]]
   * </p>
   *
   * @return The Content-Length HTTP header indicating the size of the
   *         associated object in bytes.
   * @see ObjectMetadata#setContentLength(long)
   */
  lazy val contentLength: Long =
    metadata
      .collectFirst {
        case cl: `Content-Length` => cl.length
      }
      .getOrElse(0)

  /**
   * Java Api
   *
   * <p>
   * Gets the Content-Length HTTP header indicating the size of the
   * associated object in bytes.
   * </p>
   * <p>
   * This field is required when uploading objects to S3, but the AWS S3 Java
   * client will automatically set it when working directly with files. When
   * uploading directly from a stream, set this field if
   * possible. Otherwise the client must buffer the entire stream in
   * order to calculate the content length before sending the data to
   * Amazon S3.
   * </p>
   * <p>
   * For more information on the Content-Length HTTP header, see
   * [[https://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html#sec14.13]]
   * </p>
   *
   * @return The Content-Length HTTP header indicating the size of the
   *         associated object in bytes.
   * @see ObjectMetadata#setContentLength(long)
   */
  def getContentLength: Long =
    contentLength

  /**
   * <p>
   * Gets the Content-Type HTTP header, which indicates the type of content
   * stored in the associated object. The value of this header is a standard
   * MIME type.
   * </p>
   * <p>
   * When uploading files, the AWS S3 Java client will attempt to determine
   * the correct content type if one hasn't been set yet. Users are
   * responsible for ensuring a suitable content type is set when uploading
   * streams. If no content type is provided and cannot be determined by
   * the filename, the default content type, "application/octet-stream", will
   * be used.
   * </p>
   * <p>
   * For more information on the Content-Type header, see
   * [[https://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html#sec14.17]]
   * </p>
   *
   * @return The HTTP Content-Type header, indicating the type of content
   *         stored in the associated S3 object.
   * @see ObjectMetadata#setContentType(String)
   */
  lazy val contentType: Option[String] = metadata.collectFirst {
    case ct: `Content-Type` => ct.value
  }

  /**
   * Java Api
   *
   * <p>
   * Gets the Content-Type HTTP header, which indicates the type of content
   * stored in the associated object. The value of this header is a standard
   * MIME type.
   * </p>
   * <p>
   * When uploading files, the AWS S3 Java client will attempt to determine
   * the correct content type if one hasn't been set yet. Users are
   * responsible for ensuring a suitable content type is set when uploading
   * streams. If no content type is provided and cannot be determined by
   * the filename, the default content type, "application/octet-stream", will
   * be used.
   * </p>
   * <p>
   * For more information on the Content-Type header, see
   * [[https://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html#sec14.17]]
   * </p>
   *
   * @return The HTTP Content-Type header, indicating the type of content
   *         stored in the associated S3 object.
   * @see ObjectMetadata#setContentType(String)
   */
  def getContentType: Optional[String] =
    contentType.toJava

  /**
   * Gets the value of the Last-Modified header, indicating the date
   * and time at which Amazon S3 last recorded a modification to the
   * associated object.
   *
   * @return The date and time at which Amazon S3 last recorded a modification
   *         to the associated object.
   */
  lazy val lastModified: DateTime = metadata.collectFirst {
    case ct: `Last-Modified` => ct.date
  }.get

  /**
   * Java Api
   *
   * Gets the value of the Last-Modified header, indicating the date
   * and time at which Amazon S3 last recorded a modification to the
   * associated object.
   *
   * @return The date and time at which Amazon S3 last recorded a modification
   *         to the associated object.
   */
  def getLastModified: DateTime =
    lastModified

  /**
   * Gets the optional Cache-Control header
   */
  lazy val cacheControl: Option[String] = metadata.collectFirst {
    case c: `Cache-Control` => c.value
  }

  /**
   * Java Api
   *
   * Gets the optional Cache-Control header
   */
  def getCacheControl: Optional[String] =
    cacheControl.toJava

  /**
   * Gets the value of the version id header. The version id will only be available
   * if the versioning is enabled in the bucket
   *
   * @return optional version id of the object
   */
  lazy val versionId: Option[String] = metadata.collectFirst {
    case v if v.lowercaseName() == "x-amz-version-id" => v.value()
  }

  /**
   * Java Api
   *
   * Gets the value of the version id header. The version id will only be available
   * if the versioning is enabled in the bucket
   *
   * @return optional version id of the object
   */
  def getVersionId: Optional[String] = versionId.toJava

}
object ObjectMetadata {
  def apply(metadata: Seq[HttpHeader]) = new ObjectMetadata(metadata)
}

/**
 * While checking for bucket access those responses are available
 * 1) AccessDenied - User does have permission to perform ListBucket operation, so bucket exits
 * 2) AccessGranted - User doesn't have rights to perform ListBucket but bucket exits
 * 3) NotExists - Bucket doesn't exit
 *
 * @see https://docs.aws.amazon.com/AmazonS3/latest/API/RESTBucketHEAD.html
 */
sealed trait BucketAccess

object BucketAccess {
  case object AccessDenied extends BucketAccess
  case object AccessGranted extends BucketAccess
  case object NotExists extends BucketAccess

  val accessDenied: BucketAccess = AccessDenied
  val accessGranted: BucketAccess = AccessGranted
  val notExists: BucketAccess = NotExists
}

/**
 * https://docs.aws.amazon.com/AmazonS3/latest/dev/BucketRestrictions.html
 * https://docs.aws.amazon.com/AmazonS3/latest/dev/UsingMetadata.html
 */
object BucketAndKey {
  private val bucketRegexPathStyle = "(/\\.\\.)|(\\.\\./)".r
  private val bucketRegexDns = "[^a-z0-9\\-\\.]{1,255}|[\\.]{2,}".r

  def pathStyleValid(bucket: String) = {
    bucketRegexPathStyle.findFirstIn(bucket).isEmpty && ".." != bucket
  }

  def dnsValid(bucket: String) = {
    bucketRegexDns.findFirstIn(bucket).isEmpty
  }

  private[s3] def validateBucketName(bucket: String, conf: S3Settings): Unit = {
    if (conf.accessStyle == PathAccessStyle) {
      if (!pathStyleValid(bucket)) {
        throw IllegalUriException(
          "The bucket name contains sub-dir selection with `..`",
          "Selecting sub-directories with `..` is forbidden (and won't work with non-path-style access)."
        )
      }
    } else {
      bucketRegexDns.findFirstIn(bucket) match {
        case Some(illegalCharacter) =>
          throw IllegalUriException(
            "Bucket name contains non-LDH characters",
            s"The following character is not allowed: $illegalCharacter"
          )
        case None => ()
      }
    }
  }

  def objectKeyValid(key: String): Boolean = !key.split("/").contains("..")

  private[s3] def validateObjectKey(key: String, conf: S3Settings): Unit = {
    if (conf.validateObjectKey && !objectKeyValid(key))
      throw IllegalUriException(
        "The object key contains sub-dir selection with `..`",
        "Selecting sub-directories with `..` is forbidden (see the `validate-object-key` setting)."
      )

  }

}
