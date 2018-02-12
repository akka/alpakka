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

  final case class BucketInfo(
      name: String,
      location: String,
      kind: Option[String] = None,
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
