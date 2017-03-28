/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.s3.auth

import java.net.{ URLDecoder, URLEncoder }

import akka.http.scaladsl.model.Uri.{Path, Query}
import akka.http.scaladsl.model.{HttpHeader, HttpRequest}

import scala.util.Try

// Documentation: http://docs.aws.amazon.com/general/latest/gr/sigv4-create-canonical-request.html
private[alpakka] case class CanonicalRequest(method: String,
                                             uri: String,
                                             queryString: String,
                                             headerString: String,
                                             signedHeaders: String,
                                             hashedPayload: String) {
  def canonicalString: String = s"$method\n$uri\n$queryString\n$headerString\n\n$signedHeaders\n$hashedPayload"
}

private[alpakka] object CanonicalRequest {

  def from(req: HttpRequest): CanonicalRequest = {
    val hashedBody = req.headers.find(_.name == "x-amz-content-sha256").map(_.value).getOrElse("")
    from(req, hashedBody)
  }

  def from(req: HttpRequest, hashedBody: String): CanonicalRequest =
    CanonicalRequest(
      req.method.value,
      preprocessPath(req.uri.path),
      canonicalQueryString(req.uri.query()),
      canonicalHeaderString(req.headers),
      signedHeadersString(req.headers),
      hashedBody
    )

  def canonicalQueryString(query: Query): String =
    query.sortBy(_._1).map { case (a, b) => encodeQueryParams(a, b) }.mkString("&")

  private def encodeQueryParams(name: String, value: String): String =
    if (name.contains(' ')) {
      uriEncode(name.takeWhile(_ != ' ')).replace("%7E", "~") + "="
    } else {
      uriEncode(name).replace("%7E", "~") + "=" + uriEncode(value).replace("%7E", "~")
    }

  private def uriEncode(str: String) = if (isAlreadyURLEncoded(str)) str else URLEncoder.encode(str, "utf-8")

  // TODO: this could be removed when the Uri class in akka-http will accept utf-8. See https://github.com/akka/akka-http/issues/86
  private def isAlreadyURLEncoded(str: String): Boolean =
    Try {
      URLDecoder.decode(str, "utf-8")
    }.getOrElse(str) != str

  private def removeRedundantAndRelativePath(path: String): String =
    if (path.contains("..")) "/" else path.replace("./", "")

  /**
   * URL encodes the given string.  This allows us to pass special characters
   * that would otherwise be rejected when building a URI instance.  Because we
   * need to retain the URI's path structure we subsequently need to replace
   * percent encoded path delimiters back to their decoded counterparts.
   */
  private def preprocessPath(path: Path): String =
    uriEncode(removeRedundantAndRelativePath(path.toString))
      .replace(":", "%3A")
      .replace("%2F", "/")
      .replace("%7E", "~")
      .replace("+", "%20")

  def canonicalHeaderString(headers: Seq[HttpHeader]): String = {
    val grouped = headers.groupBy(_.lowercaseName())
    val combined = grouped.mapValues(_.map(_.value.replaceAll("\\s+", " ")).mkString(","))
    fixContentTypeHeaderParameter(combined.toList.sortBy(_._1).map { case (k, v) => s"$k:$v" }.mkString("\n"))
  }

  private def fixContentTypeHeaderParameter(headers: String): String =
    headers.replace("charset=UTF-8", "charset=utf8")

  def signedHeadersString(headers: Seq[HttpHeader]): String =
    headers.map(_.lowercaseName()).distinct.sorted.mkString(";")

}
