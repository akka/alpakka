package akka.stream.alpakka.s3.signing

import java.security.MessageDigest
import java.time.format.DateTimeFormatter
import java.time.{ ZoneOffset, ZonedDateTime }

import akka.http.scaladsl.model.headers.RawHeader
import akka.http.scaladsl.model.{ HttpHeader, HttpRequest }
import akka.stream.Materializer

import scala.concurrent.{ ExecutionContext, Future }

object Signer {
  val dateFormatter = DateTimeFormatter.ofPattern("YYYYMMdd'T'HHmmssX")

  def signedRequest(request: HttpRequest, key: SigningKey, date: ZonedDateTime = ZonedDateTime.now(ZoneOffset.UTC))(implicit mat: Materializer): Future[HttpRequest] = {
    implicit val ec: ExecutionContext = mat.executionContext
    val hashedBody = request.entity.dataBytes.runWith(StreamUtils.digest()).map {
      case hash =>
        Utils.encodeHex(hash.toArray)
    }

    hashedBody.map {
      case hb =>
        val headersToAdd = Seq(RawHeader("x-amz-date", date.format(dateFormatter)), RawHeader("x-amz-content-sha256", hb)) ++ sessionHeader(key.credentials)
        val reqWithHeaders = request.withHeaders(request.headers ++ headersToAdd)
        val cr = CanonicalRequest.from(reqWithHeaders)
        val authHeader = authorizationHeader("AWS4-HMAC-SHA256", key, date, cr)
        reqWithHeaders.withHeaders(reqWithHeaders.headers ++ Seq(authHeader))
    }
  }

  def sessionHeader(creds: AWSCredentials): Option[HttpHeader] = {
    creds match {
      case bc: BasicCredentials                      => None
      case AWSSessionCredentials(_, _, sessionToken) => Some(RawHeader("X-Amz-Security-Token", sessionToken))
    }
  }

  def authorizationHeader(algorithm: String, key: SigningKey, requestDate: ZonedDateTime, canonicalRequest: CanonicalRequest): HttpHeader = {
    RawHeader("Authorization", authorizationString(algorithm, key, requestDate, canonicalRequest))
  }

  def authorizationString(algorithm: String, key: SigningKey, requestDate: ZonedDateTime, canonicalRequest: CanonicalRequest): String = {
    s"$algorithm Credential=${key.credentialString}, SignedHeaders=${canonicalRequest.signedHeaders}, Signature=${key.hexEncodedSignature(stringToSign(algorithm, key, requestDate, canonicalRequest).getBytes())}"
  }

  def stringToSign(algorithm: String, signingKey: SigningKey, requestDate: ZonedDateTime, canonicalRequest: CanonicalRequest): String = {
    val digest = MessageDigest.getInstance("SHA-256")
    val hashedRequest = Utils.encodeHex(digest.digest(canonicalRequest.canonicalString.getBytes()))
    s"$algorithm\n${requestDate.format(dateFormatter)}\n${signingKey.scope.scopeString}\n$hashedRequest"
  }

}

