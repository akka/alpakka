/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.s3.impl

import java.net.URLEncoder
import java.time.Instant

import akka.annotation.InternalApi
import akka.http.scaladsl.marshallers.xml.ScalaXmlSupport
import akka.http.scaladsl.model.{ContentTypes, HttpCharsets, MediaTypes, Uri}
import akka.http.scaladsl.unmarshalling.{FromEntityUnmarshaller, Unmarshaller}
import akka.stream.alpakka.s3._

import scala.util.Try
import scala.xml.NodeSeq

/**
 * Internal Api
 */
@InternalApi private[impl] object Marshalling {
  import ScalaXmlSupport._

  implicit val multipartUploadUnmarshaller: FromEntityUnmarshaller[MultipartUpload] = {
    nodeSeqUnmarshaller(MediaTypes.`application/xml`, ContentTypes.`application/octet-stream`) map {
      case NodeSeq.Empty => throw Unmarshaller.NoContentException
      case x =>
        MultipartUpload(S3Location((x \ "Bucket").text, (x \ "Key").text), (x \ "UploadId").text)
    }
  }

  implicit val completeMultipartUploadResultUnmarshaller: FromEntityUnmarshaller[CompleteMultipartUploadResult] = {
    nodeSeqUnmarshaller(MediaTypes.`application/xml` withCharset HttpCharsets.`UTF-8`, MediaTypes.`text/event-stream`) map {
      case NodeSeq.Empty => throw Unmarshaller.NoContentException
      case x =>
        CompleteMultipartUploadResult(
          Try(Uri((x \ "Location").text))
            .getOrElse(Uri((x \ "Location").text.split("/").map(s => URLEncoder.encode(s, "utf-8")).mkString("/"))),
          (x \ "Bucket").text,
          (x \ "Key").text,
          (x \ "ETag").text.drop(1).dropRight(1)
        )
    }
  }

  val isTruncated = "IsTruncated"
  val apiV2ContinuationToken = "NextContinuationToken"

  implicit val listBucketResultUnmarshaller: FromEntityUnmarshaller[ListBucketResult] = {
    nodeSeqUnmarshaller(MediaTypes.`application/xml` withCharset HttpCharsets.`UTF-8`).map {
      case NodeSeq.Empty => throw Unmarshaller.NoContentException
      case x =>
        val truncated = (x \ isTruncated).text == "true"
        val continuation = if (truncated) {
          Some(x \ apiV2ContinuationToken)
            .filter(_.nonEmpty)
            .orElse((x \\ "Contents" \ "Key").lastOption)
            .map(_.text)
        } else None
        ListBucketResult(
          truncated,
          continuation,
          (x \\ "Contents").map { c =>
            ListBucketResultContents(
              (x \ "Name").text,
              (c \ "Key").text,
              Utils.removeQuotes((c \ "ETag").text),
              (c \ "Size").text.toLong,
              Instant.parse((c \ "LastModified").text),
              (c \ "StorageClass").text
            )
          },
          (x \\ "CommonPrefixes").map { c =>
            ListBucketResultCommonPrefixes(
              (x \ "Name").text,
              (c \ "Prefix").text
            )
          }
        )
    }
  }

  implicit val copyPartResultUnmarshaller: FromEntityUnmarshaller[CopyPartResult] = {
    nodeSeqUnmarshaller(MediaTypes.`application/xml`, ContentTypes.`application/octet-stream`) map {
      case NodeSeq.Empty => throw Unmarshaller.NoContentException
      case x =>
        val lastModified = Instant.parse((x \ "LastModified").text)
        val eTag = (x \ "ETag").text
        CopyPartResult(lastModified, Utils.removeQuotes(eTag))
    }
  }

  implicit val listMultipartUploadsResultUnmarshaller: FromEntityUnmarshaller[ListMultipartUploadsResult] = {
    nodeSeqUnmarshaller(MediaTypes.`application/xml` withCharset HttpCharsets.`UTF-8`).map {
      case NodeSeq.Empty => throw Unmarshaller.NoContentException
      case x =>
        val bucket = (x \ "Bucket").text
        val keyMarker = (x \ "KeyMarker").headOption.flatMap(x => Utils.emptyStringToOption(x.text))
        val uploadIdMarker = (x \ "UploadIdMarker").headOption.flatMap(x => Utils.emptyStringToOption(x.text))
        val nextKeyMarker = (x \ "NextKeyMarker").headOption.flatMap(x => Utils.emptyStringToOption(x.text))
        val nextUploadIdMarker = (x \ "NextUploadIdMarker").headOption.flatMap(x => Utils.emptyStringToOption(x.text))
        val delimiter = (x \ "Delimiter").headOption.flatMap(x => Utils.emptyStringToOption(x.text))
        val maxUploads = (x \ "MaxUploads").text.toInt
        val truncated = (x \ isTruncated).text == "true"
        val uploads = (x \\ "Upload").map { u =>
          val key = (u \ "Key").text
          val uploadId = (u \ "UploadId").text

          val initiator = (u \\ "Initiator").map { i =>
            val id = (i \ "ID").text
            val displayName = (i \ "DisplayName").text
            AWSIdentity(id, displayName)
          }.headOption

          val owner = (u \\ "Owner").map { o =>
            val id = (o \ "ID").text
            val displayName = (o \ "DisplayName").text
            AWSIdentity(id, displayName)
          }.headOption

          val storageClass = (u \ "StorageClass").text
          val initiated = Instant.parse((u \ "Initiated").text)

          ListMultipartUploadResultUploads(key, uploadId, initiator, owner, storageClass, initiated)
        }

        val commonPrefixes = (x \ "CommonPrefixes").map { cp =>
          CommonPrefixes((cp \ "Prefix").text)
        }

        ListMultipartUploadsResult(bucket,
                                   keyMarker,
                                   uploadIdMarker,
                                   nextKeyMarker,
                                   nextUploadIdMarker,
                                   delimiter,
                                   maxUploads,
                                   truncated,
                                   uploads,
                                   commonPrefixes)
    }
  }

  implicit val listPartsResultUnmarshaller: FromEntityUnmarshaller[ListPartsResult] = {
    nodeSeqUnmarshaller(MediaTypes.`application/xml` withCharset HttpCharsets.`UTF-8`).map {
      case NodeSeq.Empty => throw Unmarshaller.NoContentException
      case x =>
        val bucket = (x \ "Bucket").text
        val key = (x \ "Key").text
        val uploadId = (x \ "UploadId").text
        val partNumberMarker =
          (x \ "PartNumberMarker").headOption.flatMap(x => Utils.emptyStringToOption(x.text)).map(_.toInt)
        val nextPartNumberMarker =
          (x \ "NextPartNumberMarker").headOption.flatMap(x => Utils.emptyStringToOption(x.text)).map(_.toInt)

        val maxParts = (x \ "MaxParts").text.toInt
        val truncated = (x \ isTruncated).text == "true"

        val parts = (x \\ "Part").map { u =>
          val lastModified = Instant.parse((u \ "LastModified").text)
          val eTag = (u \ "ETag").text
          val partNumber = (u \\ "PartNumber").text.toInt
          val size = (u \\ "Size").text.toLong

          ListPartsResultParts(lastModified, eTag, partNumber, size)
        }

        val initiator = (x \\ "Initiator").map { i =>
          val id = (i \ "ID").text
          val displayName = (i \ "DisplayName").text
          AWSIdentity(id, displayName)
        }.headOption

        val owner = (x \\ "Owner").map { o =>
          val id = (o \ "ID").text
          val displayName = (o \ "DisplayName").text
          AWSIdentity(id, displayName)
        }.headOption

        val storageClass = (x \ "StorageClass").text

        ListPartsResult(
          bucket,
          key,
          uploadId,
          partNumberMarker,
          nextPartNumberMarker,
          maxParts,
          truncated,
          parts,
          initiator,
          owner,
          storageClass
        )
    }
  }

  implicit val listObjectVersionsResultUnmarshaller: FromEntityUnmarshaller[ListObjectVersionsResult] = {
    nodeSeqUnmarshaller(MediaTypes.`application/xml` withCharset HttpCharsets.`UTF-8`).map {
      case NodeSeq.Empty => throw Unmarshaller.NoContentException
      case x =>
        val bucket = (x \ "Bucket").text
        val name = (x \ "Name").text
        val prefix =
          (x \ "Prefix").headOption.flatMap(x => Utils.emptyStringToOption(x.text))
        val keyMarker =
          (x \ "KeyMarker").headOption.flatMap(x => Utils.emptyStringToOption(x.text))
        val nextKeyMarker =
          (x \ "NextKeyMarker").headOption.flatMap(x => Utils.emptyStringToOption(x.text))
        val versionIdMarker =
          (x \ "VersionIdMarker").headOption.flatMap(x => Utils.emptyStringToOption(x.text))
        val nextVersionIdMarker =
          (x \ "NextVersionIdMarker").headOption.flatMap(x => Utils.emptyStringToOption(x.text))
        val delimiter =
          (x \ "Delimiter").headOption.flatMap(x => Utils.emptyStringToOption(x.text))
        val maxKeys = (x \ "MaxKeys").text.toInt
        val truncated = (x \ isTruncated).text == "true"

        val versions = (x \\ "Version").map { v =>
          val eTag = (v \ "ETag").text
          val isLatest = (v \ "IsLatest").text == "true"
          val key = (v \ "Key").text
          val lastModified = Instant.parse((v \ "LastModified").text)
          val owner = (v \\ "Owner").map { o =>
            val id = (o \ "ID").text
            val displayName = (o \ "DisplayName").text
            AWSIdentity(id, displayName)
          }.headOption
          val size = (v \\ "Size").text.toLong
          val storageClass = (v \ "StorageClass").text
          val versionId = (v \ "VersionId").headOption.flatMap(x => Utils.emptyStringToOption(x.text))
          ListObjectVersionsResultVersions(eTag, isLatest, key, lastModified, owner, size, storageClass, versionId)
        }

        val commonPrefixes = (x \ "CommonPrefixes").map { cp =>
          CommonPrefixes((cp \ "Prefix").text)
        }

        val deleteMarkers = (x \\ "DeleteMarker").map { d =>
          val isLatest = (d \ "IsLatest").text == "true"
          val key = (d \ "Key").text
          val lastModified = Instant.parse((d \ "LastModified").text)
          val owner = (d \\ "Owner").map { o =>
            val id = (o \ "ID").text
            val displayName = (o \ "DisplayName").text
            AWSIdentity(id, displayName)
          }.headOption
          val versionId = (d \ "VersionId").headOption.flatMap(x => Utils.emptyStringToOption(x.text))
          DeleteMarkers(isLatest, key, lastModified, owner, versionId)
        }

        ListObjectVersionsResult(bucket,
                                 name,
                                 prefix,
                                 keyMarker,
                                 nextKeyMarker,
                                 versionIdMarker,
                                 nextVersionIdMarker,
                                 delimiter,
                                 maxKeys,
                                 truncated,
                                 versions,
                                 commonPrefixes,
                                 deleteMarkers)
    }
  }
}
