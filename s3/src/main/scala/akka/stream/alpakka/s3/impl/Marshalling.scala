/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.s3.impl

import java.time.Instant

import akka.http.scaladsl.marshallers.xml.ScalaXmlSupport
import akka.http.scaladsl.model.{ContentTypes, HttpCharsets, MediaTypes}
import akka.http.scaladsl.unmarshalling.{FromEntityUnmarshaller, Unmarshaller}
import akka.stream.alpakka.s3.scaladsl.ListBucketResultContents

import scala.xml.NodeSeq

private[alpakka] object Marshalling {
  import ScalaXmlSupport._

  implicit val multipartUploadUnmarshaller: FromEntityUnmarshaller[MultipartUpload] = {
    nodeSeqUnmarshaller(MediaTypes.`application/xml`, ContentTypes.`application/octet-stream`) map {
      case NodeSeq.Empty => throw Unmarshaller.NoContentException
      case x =>
        MultipartUpload(S3Location((x \ "Bucket").text, (x \ "Key").text), (x \ "UploadId").text)
    }
  }

  implicit val completeMultipartUploadResultUnmarshaller: FromEntityUnmarshaller[CompleteMultipartUploadResult] = {
    nodeSeqUnmarshaller(MediaTypes.`application/xml` withCharset HttpCharsets.`UTF-8`) map {
      case NodeSeq.Empty => throw Unmarshaller.NoContentException
      case x =>
        CompleteMultipartUploadResult(
          (x \ "Location").text,
          (x \ "Bucket").text,
          (x \ "Key").text,
          (x \ "ETag").text.drop(1).dropRight(1)
        )
    }
  }

  val isTruncated = "IsTruncated"
  val continuationToken = "NextContinuationToken"

  implicit val listBucketResultUnmarshaller: FromEntityUnmarshaller[ListBucketResult] = {
    nodeSeqUnmarshaller(MediaTypes.`application/xml` withCharset HttpCharsets.`UTF-8`).map {
      case NodeSeq.Empty => throw Unmarshaller.NoContentException
      case x =>
        ListBucketResult(
          (x \ isTruncated).text == "true",
          Some(x \ continuationToken).filter(_.nonEmpty).map(_.text),
          (x \\ "Contents").map { c =>
            ListBucketResultContents(
              (x \ "Name").text,
              (c \ "Key").text,
              (c \ "ETag").text.drop(1).dropRight(1),
              (c \ "Size").text.toLong,
              Instant.parse((c \ "LastModified").text),
              (c \ "StorageClass").text
            )
          }
        )
    }
  }
}
