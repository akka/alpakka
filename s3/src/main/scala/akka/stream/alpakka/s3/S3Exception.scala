/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.s3

import scala.xml.{Elem, XML}

class S3Exception(val code: String, val message: String, val requestID: String, val hostId: String)
    extends RuntimeException(message) {

  def this(xmlResponse: Elem) =
    this((xmlResponse \ "Code").text,
         (xmlResponse \ "Message").text,
         (xmlResponse \ "RequestID").text,
         (xmlResponse \ "HostID").text)

  def this(response: String) = this(XML.loadString(response))
}
