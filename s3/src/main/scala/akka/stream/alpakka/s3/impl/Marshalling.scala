/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.s3.impl

import java.net.URLEncoder
import java.time.Instant

import akka.annotation.InternalApi
import akka.http.scaladsl.marshallers.xml.ScalaXmlSupport
import akka.http.scaladsl.model.{ContentTypes, HttpCharsets, MediaTypes, Uri}
import akka.http.scaladsl.unmarshalling.{FromEntityUnmarshaller, Unmarshaller}
import akka.stream.alpakka.s3.{ListBucketResultCommonPrefixes, ListBucketResultContents}

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
    nodeSeqUnmarshaller(MediaTypes.`application/xml` withCharset HttpCharsets.`UTF-8`,
                        MediaTypes.`text/event-stream`
    ) map {
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
              (c \ "ETag").text.drop(1).dropRight(1),
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
        CopyPartResult(lastModified, eTag.dropRight(1).drop(1))
    }
  }
}
