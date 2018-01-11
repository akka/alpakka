/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.s3.auth

import com.amazonaws.auth
import com.amazonaws.auth.{BasicAWSCredentials, BasicSessionCredentials}

@deprecated("use com.amazonaws.auth.* entities", "0.11")
sealed trait AWSCredentials {
  def accessKeyId: String
  def secretAccessKey: String

  def toAmazonCredentials(): auth.AWSCredentials =
    this match {
      case BasicCredentials(ak, sk) ⇒
        new BasicAWSCredentials(ak, sk)
      case AWSSessionCredentials(ak, sk, tok) ⇒
        new BasicSessionCredentials(ak, sk, tok)
    }
}

@deprecated("use com.amazonaws.auth.* entities", "0.11")
final case class BasicCredentials(accessKeyId: String, secretAccessKey: String) extends AWSCredentials

@deprecated("use com.amazonaws.auth.* entities", "0.11")
final case class AWSSessionCredentials(accessKeyId: String, secretAccessKey: String, sessionToken: String)
    extends AWSCredentials

object AWSCredentials {
  @deprecated("use com.amazonaws.auth.* entities", "0.11")
  def apply(accessKeyId: String, secretAccessKey: String): BasicCredentials =
    BasicCredentials(accessKeyId, secretAccessKey)

  @deprecated("use com.amazonaws.auth.* entities", "0.11")
  def create(accessKeyId: String, secretAccessKey: String): BasicCredentials =
    apply(accessKeyId, secretAccessKey)
}
