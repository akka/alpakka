/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.s3.auth

sealed trait AWSCredentials {
  def accessKeyId: String
  def secretAccessKey: String
}

final case class BasicCredentials(accessKeyId: String, secretAccessKey: String) extends AWSCredentials
final case class AWSSessionCredentials(accessKeyId: String, secretAccessKey: String, sessionToken: String)
    extends AWSCredentials

object AWSCredentials {
  def apply(accessKeyId: String, secretAccessKey: String): BasicCredentials =
    BasicCredentials(accessKeyId, secretAccessKey)
  def create(accessKeyId: String, secretAccessKey: String): BasicCredentials =
    apply(accessKeyId, secretAccessKey)
}
