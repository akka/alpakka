/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka
package azure
package storage
package headers

import akka.annotation.InternalApi
import akka.http.scaladsl.model.HttpHeader
import akka.http.scaladsl.model.headers.RawHeader

import java.security.MessageDigest
import java.util.{Base64, Objects}

sealed abstract class ServerSideEncryption {
  @InternalApi private[storage] def headers: Seq[HttpHeader]
}

object ServerSideEncryption {
  def customerKey(key: String, hash: Option[String]): ServerSideEncryption = new CustomerKey(key, hash)
  def customerKey(key: String): ServerSideEncryption = customerKey(key, None)
}

final class CustomerKey private[headers] (val key: String, val hash: Option[String] = None)
    extends ServerSideEncryption {
  override private[storage] def headers: Seq[HttpHeader] = Seq(
    RawHeader("x-ms-encryption-algorithm", "AES256"),
    RawHeader("x-ms-encryption-key", key),
    RawHeader("x-ms-encryption-key-sha256", hash.getOrElse(createHash))
  )

  override def equals(obj: Any): Boolean =
    obj match {
      case other: CustomerKey => key == other.key && hash == other.hash
      case _ => false
    }

  override def hashCode(): Int = Objects.hash(key, hash)

  override def toString: String =
    s"""ServerSideEncryption.CustomerKeys(
      |key=$key,
      | hash=$hash
      |)
      |""".stripMargin.replaceAll(System.lineSeparator(), "")

  private def createHash = {
    val messageDigest = MessageDigest.getInstance("SHA-256")
    val decodedKey = messageDigest.digest(Base64.getDecoder.decode(key))
    Base64.getEncoder.encodeToString(decodedKey)
  }
}
