/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.s3.impl

import akka.http.scaladsl.marshallers.xml.ScalaXmlSupport
import akka.http.scaladsl.model.{ContentTypes, HttpCharsets, MediaTypes}
import akka.http.scaladsl.unmarshalling.{FromEntityUnmarshaller, Unmarshaller}

import scala.xml.NodeSeq

private[alpakka] object Marshalling {
  import ScalaXmlSupport._

  implicit val multipartUploadUnmarshaller: FromEntityUnmarshaller[MultipartUpload] = {
    nodeSeqUnmarshaller(ContentTypes.`application/octet-stream`) map {
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
  val key = "Key"

  implicit val listBucketResultUnmarshaller: FromEntityUnmarshaller[ListBucketResult] = {
    nodeSeqUnmarshaller(MediaTypes.`application/xml` withCharset HttpCharsets.`UTF-8`).map {
      case NodeSeq.Empty => throw Unmarshaller.NoContentException
      case x =>
        ListBucketResult(
          (x \ isTruncated).text == "true",
          if ((x \ continuationToken).isEmpty) None else Some((x \ continuationToken).text),
          (x \\ key).toSeq.map(_.text)
        )
    }
  }
}
