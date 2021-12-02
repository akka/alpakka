/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.s3

import akka.annotation.{DoNotInherit, InternalApi}
import akka.http.scaladsl.model.StatusCode

import scala.xml.XML

/**
 * Represents AWS S3 error responses [[https://docs.aws.amazon.com/AmazonS3/latest/API/ErrorResponses.html]].
 */
@DoNotInherit
class S3Exception @InternalApi private[s3] (val statusCode: StatusCode,
                                            val code: String,
                                            val message: String,
                                            val requestId: String,
                                            val resource: String)
    extends RuntimeException(message) {

  override def toString: String =
    s"${super.toString} (Status code: $statusCode, Code: $code, RequestId: $requestId, Resource: $resource)"

}

object S3Exception {
  def apply(response: String, statusCode: StatusCode): S3Exception = {
    try {
      val xmlResponse = XML.loadString(response)
      new S3Exception(
        statusCode,
        (xmlResponse \ "Code").text,
        (xmlResponse \ "Message").text,
        (xmlResponse \ "RequestId").text,
        (xmlResponse \ "Resource").text
      )
    } catch {
      case e: Exception =>
        new S3Exception(statusCode, statusCode.toString, response, "-", "-")
    }
  }
}
