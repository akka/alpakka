/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.s3.auth

import java.net.URLEncoder

import akka.http.scaladsl.model.Uri.{Path, Query}
import akka.http.scaladsl.model.{HttpHeader, HttpRequest}

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
    CanonicalRequest(
      req.method.value,
      pathEncode(req.uri.path),
      canonicalQueryString(req.uri.query()),
      canonicalHeaderString(req.headers),
      signedHeadersString(req.headers),
      hashedBody
    )
  }

  def canonicalQueryString(query: Query): String =
    query.sortBy(_._1).map { case (a, b) => s"${uriEncode(a)}=${uriEncode(b)}" }.mkString("&")

  private def uriEncode(str: String) = URLEncoder.encode(str, "utf-8")

  def canonicalHeaderString(headers: Seq[HttpHeader]): String = {
    val grouped = headers.groupBy(_.lowercaseName())
    val combined = grouped.mapValues(_.map(_.value.replaceAll("\\s+", " ").trim).mkString(","))
    combined.toList.sortBy(_._1).map { case (k, v) => s"$k:$v" }.mkString("\n")
  }

  def signedHeadersString(headers: Seq[HttpHeader]): String =
    headers.map(_.lowercaseName()).distinct.sorted.mkString(";")

  private def pathEncode(path: Path): String =
    if (path.isEmpty) "/"
    else
      path.toString().flatMap {
        case ch if "!$&'()*+,;:=".contains(ch) => "%" + Integer.toHexString(ch.toInt).toUpperCase
        case other => other.toString
      }

}
