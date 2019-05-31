/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.s3.impl.auth

import java.security.MessageDigest
import java.time.format.DateTimeFormatter
import java.time.ZonedDateTime

import akka.NotUsed
import akka.annotation.InternalApi
import akka.http.scaladsl.model.headers.{RawHeader, `Raw-Request-URI`}
import akka.http.scaladsl.model.{HttpHeader, HttpRequest}
import akka.stream.scaladsl.Source

@InternalApi private[impl] object Signer {
  private val dateFormatter = DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmssX")

  def signedRequest(request: HttpRequest, key: SigningKey): Source[HttpRequest, NotUsed] = {
    val hashedBody = request.entity.dataBytes.via(digest()).map(hash => encodeHex(hash.toArray))

    hashedBody
      .map { hb =>
        val headersToAdd = Vector(RawHeader("x-amz-date", key.requestDate.format(dateFormatter)),
                                  RawHeader("x-amz-content-sha256", hb)) ++ sessionHeader(key)
        val reqWithHeaders = request.withHeaders(request.headers ++ headersToAdd)
        val cr = CanonicalRequest.from(
          reqWithHeaders.withHeaders(reqWithHeaders.headers.filterNot(_.isInstanceOf[`Raw-Request-URI`]))
        )
        val authHeader = authorizationHeader("AWS4-HMAC-SHA256", key, key.requestDate, cr)
        reqWithHeaders.withHeaders(reqWithHeaders.headers :+ authHeader)
      }
      .mapMaterializedValue(_ => NotUsed)
  }

  private[this] def sessionHeader(key: SigningKey): Option[HttpHeader] =
    key.sessionToken.map(RawHeader("X-Amz-Security-Token", _))

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
