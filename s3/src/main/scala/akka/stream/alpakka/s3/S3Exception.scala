/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.s3

import akka.annotation.{DoNotInherit, InternalApi}
import akka.http.scaladsl.model.{StatusCode, StatusCodes}

import scala.util.Try
import scala.xml.{Elem, XML}

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

  @deprecated("kept for binary compatiblity", "2.0.1")
  private[s3] val hostId = ""

  @deprecated("kept for binary compatiblity", "2.0.1")
  private[s3] def this(code: String, message: String, requestId: String, resource: String) =
    this(StatusCodes.NotFound, code, message, requestId, resource)

  @deprecated("kept for binary compatiblity", "2.0.1")
  private[s3] def this(xmlResponse: Elem) =
    this(StatusCodes.NotFound,
         (xmlResponse \ "Code").text,
         (xmlResponse \ "Message").text,
         (xmlResponse \ "RequestID").text,
         (xmlResponse \ "HostID").text)

  @deprecated("kept for binary compatiblity", "2.0.1")
  private[s3] def this(response: String) =
    this(
      Try(XML.loadString(response)).getOrElse(
        <Error><Code>-</Code><Message>{response}</Message><RequestId>-</RequestId><Resource>-</Resource></Error>
      )
    )

  @deprecated("kept for binary compatiblity", "2.0.1")
  private[s3] def this(response: String, code: StatusCode) =
    this(
      Try(XML.loadString(response)).getOrElse(
        <Error><Code>{code}</Code><Message>{response}</Message><RequestId>-</RequestId><Resource>-</Resource></Error>
      )
    )

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
