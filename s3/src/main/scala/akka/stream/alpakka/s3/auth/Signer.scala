/*
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.s3.auth

import java.security.MessageDigest
import java.time.format.DateTimeFormatter
import java.time.{ ZoneOffset, ZonedDateTime }

import akka.http.scaladsl.model.headers.RawHeader
import akka.http.scaladsl.model.{ HttpHeader, HttpRequest }
import akka.stream.Materializer

import scala.concurrent.Future

private[alpakka] object Signer {
  private val dateFormatter = DateTimeFormatter.ofPattern("YYYYMMdd'T'HHmmssX")

  def signedRequest(request: HttpRequest, key: SigningKey, date: ZonedDateTime = ZonedDateTime.now(ZoneOffset.UTC))(
      implicit mat: Materializer): Future[HttpRequest] = {
    import mat.executionContext
    val hashedBody = request.entity.dataBytes.runWith(digest()).map {
      case hash => encodeHex(hash.toArray)
    }

    hashedBody.map {
      case hb =>
        val headersToAdd = Vector(RawHeader("x-amz-date", date.format(dateFormatter)),
            RawHeader("x-amz-content-sha256", hb)) ++ sessionHeader(key.credentials)
        val reqWithHeaders = request.withHeaders(request.headers ++ headersToAdd)
        val cr = CanonicalRequest.from(reqWithHeaders)
        val authHeader = authorizationHeader("AWS4-HMAC-SHA256", key, date, cr)
        reqWithHeaders.withHeaders(reqWithHeaders.headers :+ authHeader)
    }
  }

  private[this] def sessionHeader(creds: AWSCredentials): Option[HttpHeader] = creds match {
    case _: BasicCredentials => None
    case AWSSessionCredentials(_, _, sessionToken) => Some(RawHeader("X-Amz-Security-Token", sessionToken))
  }

  private[this] def authorizationHeader(algorithm: String,
                                        key: SigningKey,
                                        requestDate: ZonedDateTime,
                                        canonicalRequest: CanonicalRequest): HttpHeader =
    RawHeader("Authorization", authorizationString(algorithm, key, requestDate, canonicalRequest))

  private[this] def authorizationString(algorithm: String,
                                        key: SigningKey,
                                        requestDate: ZonedDateTime,
                                        canonicalRequest: CanonicalRequest): String = {
    val sign = key.hexEncodedSignature(stringToSign(algorithm, key, requestDate, canonicalRequest).getBytes())
    s"$algorithm Credential=${key.credentialString}, SignedHeaders=${canonicalRequest.signedHeaders}, Signature=$sign"
  }

  def stringToSign(algorithm: String,
                   signingKey: SigningKey,
                   requestDate: ZonedDateTime,
                   canonicalRequest: CanonicalRequest): String = {
    val digest = MessageDigest.getInstance("SHA-256")
    val hashedRequest = encodeHex(digest.digest(canonicalRequest.canonicalString.getBytes()))
    val date = requestDate.format(dateFormatter)
    val scope = signingKey.scope.scopeString
    s"$algorithm\n$date\n$scope\n$hashedRequest"
  }

}
