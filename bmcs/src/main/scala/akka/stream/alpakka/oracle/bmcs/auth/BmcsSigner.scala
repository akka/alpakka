/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.oracle.bmcs.auth

import java.nio.charset.StandardCharsets
import java.security.Signature
import java.security.interfaces.RSAPrivateKey
import java.time.Instant
import java.util.Base64

import akka.http.scaladsl.model.headers._
import akka.http.scaladsl.model._
import akka.stream.Materializer
import akka.stream.alpakka.oracle.bmcs.BmcsException

import scala.concurrent.Future

object BmcsSigner {

  val algorithm = SignatureAlgorithm(jvmName = "SHA256withRSA", nameForAuthHeader = "rsa-sha256", version = "1")

  val requestTarget = "(request-target)"
  val xContentSha256 = "x-content-sha256"
  val genericHeaders: List[String] = List(Date.lowercaseName, Host.lowercaseName, requestTarget)
  val bodyHeaders: List[String] = List(`Content-Length`.lowercaseName, `Content-Type`.lowercaseName, xContentSha256)
  val allHeaders: List[String] = genericHeaders ::: bodyHeaders
  val requiredHeadersMap: Map[HttpMethod, Seq[String]] = Seq(
    HttpMethods.GET -> genericHeaders,
    HttpMethods.HEAD -> genericHeaders,
    HttpMethods.DELETE -> genericHeaders,
    HttpMethods.PUT -> genericHeaders,
    HttpMethods.POST -> allHeaders
  ).toMap

  /**
   * Calculates the hash of the request entity body (except for PUT requests) and
   * signs the hash. And adds the authorization header.
   *
   * For POST / GET requests the entity body will be materialized twice.
   * Object storage PUT requests are special and do not need the body hash.
   * See required headers section of.
   * https://docs.us-phoenix-1.oraclecloud.com/Content/API/Concepts/signingrequests.htm
   *
   *
   * @param request The request to add authorization signature to.
   * @param credentials credentials
   * @param algorithm algorithm used for signature.
   * @param date date to use for the [Date] header
   * @param mat materializer to use
   * @return
   */
  def signedRequest(request: HttpRequest,
                    credentials: BmcsCredentials,
                    algorithm: SignatureAlgorithm = algorithm,
                    date: Instant = Instant.now())(
      implicit mat: Materializer
  ): Future[HttpRequest] = {
    import mat.executionContext

    //these headers are just calculated for calculating signature.
    val pseudoHeaders: Seq[HttpHeader] = calculatePseudoHeaders(request)

    //these headers are actually added to the request. also calculates hash of entity body when needed.
    val headersToAdd: Future[Seq[HttpHeader]] = calculateHeadersToAdd(request, date)
    headersToAdd.map { toAdd =>
      val all = request.headers ++ toAdd ++ pseudoHeaders
      val authHeader = authorizationHeader(all, request.uri, request.method, algorithm, credentials)
      request.withHeaders(request.headers ++ toAdd :+ authHeader)
    }
  }

  private def calculateHeadersToAdd(request: HttpRequest, date: Instant = Instant.now)(
      implicit mat: Materializer
  ): Future[Seq[HttpHeader]] = {
    import mat.executionContext
    val dateHdr = Date(DateTime(date.toEpochMilli))
    request.method match {
      case HttpMethods.GET | HttpMethods.DELETE | HttpMethods.PUT | HttpMethods.HEAD =>
        Future.successful(Seq(dateHdr)) //for GET DELETE PUT and HEAD only Date header is added.
      case HttpMethods.POST =>
        request.entity.dataBytes
          .runWith(digest())
          .map(hash => encodeBase64(hash.toArray))
          .map(bh => RawHeader(xContentSha256, bh))
          .map(hd => Seq(hd, dateHdr))
      case _ => throw new UnsupportedOperationException(s"Does not support this httpMethod: ${request.method.value}")
    }
  }

  private def calculatePseudoHeaders(request: HttpRequest) = {
    val contentType = RawHeader("content-type", request.entity.contentType.value)
    val contentLength = RawHeader("content-length", request.entity.contentLengthOption.get.toString)
    val requestTarget =
      RawHeader("(request-target)", s"${request.method.value.toLowerCase} ${extractPath(request.uri)}")
    Seq(contentType, contentLength, requestTarget)
  }

  private[this] def signatureParts(headers: Seq[HttpHeader], uri: Uri, method: HttpMethod): SignatureParts = {
    val requiredHeaders: Seq[String] = requiredHeadersMap.get(method) match {
      case Some(x) => x
      case None => throw BmcsException(s"Unsupported Http Method :${method.value}")
    }
    val availableHeaders: Seq[String] = headers.map(_.lowercaseName)
    val missingHeaders = requiredHeaders.filterNot(_ == requestTarget).filterNot(availableHeaders.contains(_))
    if (missingHeaders.nonEmpty) {
      throw BmcsException(
        s"Required Headers were not found in the request. Missing Headers: ${missingHeaders.mkString(", ")}"
      )
    }

    val headersToSign: Seq[HttpHeader] = headers.filter(h => requiredHeaders.contains(h.lowercaseName))

    // Header name and value are separated with ": " and each (name, value)
    // pair is separated with "\n"
    val headerNamesAndValues = headersToSign.map(h => (h.lowercaseName, h.value))
    val stringToSign = headerNamesAndValues
      .map {
        case (key, value) => s"$key: $value"
      }
      .mkString("\n")

    //the order of headers in header parameter in auth header needs to match the order of headers in string to sign. .
    SignatureParts(stringToSign, headerOrder = headerNamesAndValues.map(_._1).mkString(" "))
  }

  private def extractPath(uri: Uri) = uri.rawQueryString match {
    case None => uri.path.toString()
    case Some(queryString) => uri.path.toString() + "?" + queryString
  }

  private[this] def sign(message: Array[Byte], privateKey: RSAPrivateKey, algorithm: SignatureAlgorithm): Array[Byte] = {
    val signature: Signature = Signature.getInstance(algorithm.jvmName)
    signature.initSign(privateKey)
    signature.update(message)
    signature.sign
  }

  private[this] def authorizationHeader(headers: Seq[HttpHeader],
                                        uri: Uri,
                                        method: HttpMethod,
                                        algorithm: SignatureAlgorithm,
                                        credentials: BmcsCredentials): HttpHeader = {
    val parts = signatureParts(headers, uri, method)
    RawHeader("Authorization", authorizationString(parts, algorithm, credentials))
  }

  private[this] def authorizationString(signatureParts: SignatureParts,
                                        algorithm: SignatureAlgorithm,
                                        credentials: BmcsCredentials): String = {
    val base64Sign = Base64.getEncoder.encodeToString(
      sign(signatureParts.stringToSign.getBytes(StandardCharsets.UTF_8), credentials.rsaPrivateKey, algorithm)
    )
    s"""Signature version="${algorithm.version}",headers="${signatureParts.headerOrder}",keyId="${credentials.keyId}",algorithm="${algorithm.nameForAuthHeader}",signature="$base64Sign""""
  }
}

case class SignatureParts(stringToSign: String, headerOrder: String)

case class SignatureAlgorithm(jvmName: String, nameForAuthHeader: String, version: String)
