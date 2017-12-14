/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.storage

import scala.collection.immutable.Seq

object Model {
  final case class StorageObject(
      kind: String,
      id: String,
      name: String,
      bucket: String,
      generation: String,
      contentType: Option[String],
      size: String,
      etag: String
  )

  // TODO add more from https://github.com/GoogleCloudPlatform/google-cloud-java/blob/master/google-cloud-storage/src/main/java/com/google/cloud/storage/BucketInfo.java
  final case class BucketInfo(
      /**
       * The bucket name
       */
      name: String,
      /**
       * The bucket's location. Data for blobs in the bucket resides in physical storage within
       * this region. A list of supported values is available
       * <a href="https://cloud.google.com/storage/docs/bucket-locations">here</a>.
       */
      location: String,
      /**
       * The kind of bucket
       */
      kind: Option[String] = None,
      /**
       * THe id of the bucket
       */
      id: Option[String] = None
  )

  final case class BucketListResult(
      kind: String,
      nextPageToken: Option[String],
      prefixes: Option[List[String]],
      items: List[StorageObject]
  ) {
    def merge(other: BucketListResult): BucketListResult =
      //todo merge prefixes
      copy(nextPageToken = None, items = this.items ++ other.items)
  }

  final class ObjectNotFoundException(err: String) extends RuntimeException(err)

  final case class MultiPartUpload(uploadId: String)

  sealed trait UploadPartResponse
  final case class SuccessfulUploadPart(multiPartUpload: MultiPartUpload, index: Int) extends UploadPartResponse
  final case class FailedUploadPart(multiPartUpload: MultiPartUpload, index: Int, exception: Throwable)
      extends UploadPartResponse
  final case class SuccessfulUpload(multiPartUpload: MultiPartUpload, index: Int, storageObject: StorageObject)
      extends UploadPartResponse

  final case class FailedUpload(reasons: Seq[Throwable]) extends Exception(reasons.map(_.getMessage).mkString(", "))
}
