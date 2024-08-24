/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka
package azure
package storage
package impl.auth

import akka.NotUsed
import akka.http.scaladsl.model.{HttpRequest, Uri}
import akka.http.scaladsl.model.headers._
import akka.stream.scaladsl.Source

import java.util.Base64
import javax.crypto.Mac
import javax.crypto.spec.SecretKeySpec

/** Takes initial request and add signed `Authorization` header and essential XMS headers.
 *
 * @param initialRequest
 *   - Initial HTTP request without authorization and XMS headers
 * @param settings
 *   - Storage settings
 */
final case class Signer(initialRequest: HttpRequest, settings: StorageSettings) {

  private val credential = settings.azureNameKeyCredential
  private val requestWithHeaders =
    initialRequest
      .addHeader(RawHeader(XmsDateHeaderKey, getFormattedDate))
      .addHeader(RawHeader(XmsVersionHeaderKey, settings.apiVersion))

  private lazy val mac = {
    val mac = Mac.getInstance(settings.algorithm)
    mac.init(new SecretKeySpec(credential.accountKey, settings.algorithm))
    mac
  }

  def signedRequest: Source[HttpRequest, NotUsed] = {
    import Signer._
    val authorizationType = settings.authorizationType
    if (authorizationType == "anon" || authorizationType == "sas") Source.single(requestWithHeaders)
    else {
      val headersToSign =
        if (authorizationType == "SharedKeyLite") buildHeadersToSign(SharedKeyLiteHeaders)
        else buildHeadersToSign(SharedKeyHeaders)

      val signature = sign(headersToSign)
      Source.single(
        requestWithHeaders.addHeader(
          RawHeader(AuthorizationHeaderKey, s"$authorizationType ${credential.accountName}:$signature")
        )
      )
    }
  }

  private def getHeaderOptionalValue(headerName: String) =
    requestWithHeaders.headers.collectFirst {
      case header if header.name() == headerName => header.value()
    }

  private def getHeaderValue(headerName: String, defaultValue: String = "") =
    getHeaderOptionalValue(headerName).getOrElse(defaultValue)

  private def getContentLengthValue = {
    val contentLengthValue = getHeaderValue(`Content-Length`.name, "0")
    if (contentLengthValue == "0") "" else contentLengthValue
  }

  private def buildHeadersToSign(headerNames: Seq[String]) = {
    def getValue(headerName: String) = {
      if (headerName == `Content-Length`.name) getContentLengthValue
      else getHeaderValue(headerName)
    }

    Seq(requestWithHeaders.method.value.toUpperCase) ++ headerNames.map(getValue) ++
    getAdditionalXmsHeaders ++ getCanonicalizedResource
  }

  private def getAdditionalXmsHeaders =
    requestWithHeaders.headers.filter(header => header.name().startsWith("x-ms-")).sortBy(_.name()).map { header =>
      s"${header.name()}:${header.value()}"
    }

  private def getCanonicalizedResource = {
    val uri = requestWithHeaders.uri
    val resourcePath = s"/${credential.accountName}${uri.path.toString()}"

    val queries =
      uri.queryString() match {
        case Some(queryString) =>
          Uri.Query(queryString).toMap.toSeq.sortBy(_._1).map {
            case (key, value) => s"$key:$value"
          }

        case None => Seq.empty[String]
      }

    Seq(resourcePath) ++ queries
  }

  private def sign(headersToSign: Seq[String]) =
    Base64.getEncoder.encodeToString(mac.doFinal(headersToSign.mkString(NewLine).getBytes))
}

object Signer {
  private val SharedKeyHeaders =
    Seq(
      `Content-Encoding`.name,
      "Content-Language",
      `Content-Length`.name,
      "Content-MD5",
      `Content-Type`.name,
      Date.name,
      `If-Modified-Since`.name,
      `If-Match`.name,
      `If-None-Match`.name,
      `If-Unmodified-Since`.name,
      Range.name
    )

  private val SharedKeyLiteHeaders = Seq("Content-MD5", `Content-Type`.name, Date.name)
}
