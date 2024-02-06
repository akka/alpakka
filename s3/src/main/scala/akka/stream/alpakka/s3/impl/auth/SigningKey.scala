/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.s3.impl.auth

import java.time.{LocalDate, ZonedDateTime}
import java.time.format.DateTimeFormatter

import akka.annotation.InternalApi
import javax.crypto.Mac
import javax.crypto.spec.SecretKeySpec
import software.amazon.awssdk.auth.credentials._
import software.amazon.awssdk.regions.Region

@InternalApi private[impl] final case class CredentialScope(date: LocalDate, awsRegion: Region, awsService: String) {
  lazy val formattedDate: String = date.format(DateTimeFormatter.BASIC_ISO_DATE)

  def scopeString = s"$formattedDate/$awsRegion/$awsService/aws4_request"
}

@InternalApi private[impl] final case class SigningKey(requestDate: ZonedDateTime,
                                                       credProvider: AwsCredentialsProvider,
                                                       scope: CredentialScope,
                                                       algorithm: String = "HmacSHA256"
) {

  private val credentials: AwsCredentials = credProvider.resolveCredentials

  def anonymous: Boolean = credentials.secretAccessKey() == None.orNull && credentials.accessKeyId() == None.orNull

  val rawKey = new SecretKeySpec(s"AWS4${credentials.secretAccessKey}".getBytes, algorithm)

  val sessionToken: Option[String] = credentials match {
    case c: AwsSessionCredentials => Some(c.sessionToken)
    case _ => None
  }

  def signature(message: Array[Byte]): Array[Byte] = signWithKey(key, message)

  def hexEncodedSignature(message: Array[Byte]): String = encodeHex(signature(message))

  def credentialString: String = s"${credentials.accessKeyId}/${scope.scopeString}"

  lazy val key: SecretKeySpec =
    wrapSignature(dateRegionServiceKey, "aws4_request".getBytes)

  lazy val dateRegionServiceKey: SecretKeySpec =
    wrapSignature(dateRegionKey, scope.awsService.getBytes)

  lazy val dateRegionKey: SecretKeySpec =
    wrapSignature(dateKey, scope.awsRegion.id.getBytes)

  lazy val dateKey: SecretKeySpec =
    wrapSignature(rawKey, scope.formattedDate.getBytes)

  private def wrapSignature(signature: SecretKeySpec, message: Array[Byte]): SecretKeySpec =
    new SecretKeySpec(signWithKey(signature, message), algorithm)

  private def signWithKey(key: SecretKeySpec, message: Array[Byte]): Array[Byte] = {
    val mac = Mac.getInstance(algorithm)
    mac.init(key)
    mac.doFinal(message)
  }
}
