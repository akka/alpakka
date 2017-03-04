/*
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.backblazeb2

import java.net.{URLDecoder, URLEncoder}
import java.nio.charset.StandardCharsets
import java.security.MessageDigest
import java.util.Base64
import javax.xml.bind.DatatypeConverter

import akka.util.ByteString

object B2Encoder {
  def encode(s: String): String =
    URLEncoder.encode(s, StandardCharsets.UTF_8.name).replace("%2F", "/")

  def decode(s: String): String =
    URLDecoder.decode(s, StandardCharsets.UTF_8.name)

  def encodeBase64(s: String): String = {
    val encoded = Base64.getEncoder.encode(s.getBytes(StandardCharsets.UTF_8))
    new String(encoded, StandardCharsets.UTF_8.name)
  }

  def encodeHex(bytes: Array[Byte]): String = DatatypeConverter.printHexBinary(bytes).toLowerCase

  def encodeHex(bytes: ByteString): String = encodeHex(bytes.toArray)

  private val SHA_1 = "SHA-1"
  def sha1(x: ByteString): ByteString = {
    val digest = MessageDigest.getInstance(SHA_1)
    digest.update(x.toByteBuffer)
    ByteString(digest.digest())
  }

  def sha1String: ByteString => String = sha1 _ andThen encodeHex

  def sha1String(x: String): String = sha1String(ByteString(x.getBytes(StandardCharsets.UTF_8.name)))
}
