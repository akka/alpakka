/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.s3

import scala.util.Try
import scala.xml.{Elem, XML}

class S3Exception(val code: String, val message: String, val requestId: String, val hostId: String)
    extends RuntimeException(message) {

  def this(xmlResponse: Elem) =
    this((xmlResponse \ "Code").text,
         (xmlResponse \ "Message").text,
         (xmlResponse \ "RequestID").text,
         (xmlResponse \ "HostID").text)

  def this(response: String) =
    this(
      Try(XML.loadString(response)).getOrElse(
        <Error><Code>-</Code><Message>{response}</Message><RequestID>-</RequestID><HostID>-</HostID></Error>
      )
    )

  override def toString = s"${super.toString} (Code: $code, RequestID: $requestId, HostID: $hostId)"
}
