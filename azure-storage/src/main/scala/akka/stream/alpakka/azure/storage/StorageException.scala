/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka
package azure
package storage

import akka.http.scaladsl.model.StatusCode

import scala.util.{Failure, Success, Try}
import scala.xml.{Elem, NodeSeq, XML}

final case class StorageException(statusCode: StatusCode,
                                  errorCode: String,
                                  errorMessage: String,
                                  resourceName: Option[String],
                                  resourceValue: Option[String],
                                  reason: Option[String])
    extends RuntimeException(errorMessage) {

  override def toString: String =
    s"""StorageException(
       |statusCode=$statusCode,
       | errorCode=$errorCode,
       | errorMessage=$errorMessage,
       | resourceName=$resourceName,
       | resourceValue=$resourceValue,
       | reason=$reason
       |)""".stripMargin.replaceAll(System.lineSeparator(), "")
}

object StorageException {
  def apply(statusCode: StatusCode,
            errorCode: String,
            errorMessage: String,
            resourceName: Option[String],
            resourceValue: Option[String],
            reason: Option[String]): StorageException =
    new StorageException(statusCode, errorCode, errorMessage, resourceName, resourceValue, reason)

  def apply(response: String, statusCode: StatusCode): StorageException = {
    def getOptionalValue(xmlResponse: Elem, elementName: String, fallBackElementName: Option[String]) = {
      val element = xmlResponse \ elementName
      val node =
        if (element.nonEmpty) element
        else if (fallBackElementName.nonEmpty) xmlResponse \ fallBackElementName.get
        else NodeSeq.Empty

      emptyStringToOption(node.text)
    }

    Try {
      val utf8_bom = "\uFEFF"
      val sanitizedResponse = if (response.startsWith(utf8_bom)) response.substring(1) else response
      val xmlResponse = XML.loadString(sanitizedResponse)
      StorageException(
        statusCode = statusCode,
        errorCode = (xmlResponse \ "Code").text,
        errorMessage = (xmlResponse \ "Message").text,
        resourceName = getOptionalValue(xmlResponse, "QueryParameterName", Some("HeaderName")),
        resourceValue = getOptionalValue(xmlResponse, "QueryParameterValue", Some("HeaderValue")),
        reason = getOptionalValue(xmlResponse, "Reason", Some("AuthenticationErrorDetail"))
      )
    } match {
      case Failure(ex) =>
        StorageException(
          statusCode = statusCode,
          errorCode = Option(ex.getMessage).getOrElse("null"),
          errorMessage = Option(response).getOrElse("null"),
          resourceName = None,
          resourceValue = None,
          reason = None
        )
      case Success(value) => value
    }

  }
}
