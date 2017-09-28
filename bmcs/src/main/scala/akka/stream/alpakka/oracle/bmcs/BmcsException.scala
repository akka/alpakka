/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.oracle.bmcs

import akka.http.scaladsl.model.HttpResponse

case class BmcsException(val code: String, val message: String, val requestID: String)
    extends RuntimeException(message) {}

object BmcsException {
  def apply(response: HttpResponse, message: String): BmcsException = {
    val code = response.status.value
    val requestID = response.headers.find(_.lowercaseName == "opc-request-id").map(_.value).getOrElse("NotFound")
    BmcsException(code, message, requestID)
  }

  def apply(message: String): BmcsException = BmcsException("NA", message, "NA")
}
