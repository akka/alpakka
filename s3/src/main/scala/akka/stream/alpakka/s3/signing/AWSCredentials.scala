package akka.stream.alpakka.s3.signing

sealed trait AWSCredentials {
  def accessKeyId: String
  def secretAccessKey: String
}

case class BasicCredentials(accessKeyId: String, secretAccessKey: String) extends AWSCredentials
case class AWSSessionCredentials(accessKeyId: String, secretAccessKey: String, sessionToken: String) extends AWSCredentials

object AWSCredentials {
  def apply(accessKeyId: String, secretAccessKey: String): BasicCredentials = {
    BasicCredentials(accessKeyId, secretAccessKey)
  }
}