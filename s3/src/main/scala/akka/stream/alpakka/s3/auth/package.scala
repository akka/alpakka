/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.s3

import java.security.MessageDigest

import akka.NotUsed
import akka.stream.scaladsl.Flow
import akka.util.ByteString

package object auth {

  private val Digits = "0123456789abcdef".toCharArray()

  def encodeHex(bytes: Array[Byte]): String = {
    val length = bytes.length
    val out = new Array[Char](length * 2)
    for (i <- 0 to length - 1) {
      val b = bytes(i)
      out(i * 2) = Digits((b >> 4) & 0xF)
      out(i * 2 + 1) = Digits(b & 0xF)
    }
    new String(out)
  }

  def encodeHex(bytes: ByteString): String = encodeHex(bytes.toArray)

  def digest(algorithm: String = "SHA-256"): Flow[ByteString, ByteString, NotUsed] =
    Flow[ByteString]
      .fold(MessageDigest.getInstance(algorithm)) {
        case (digest, bytes) =>
          digest.update(bytes.asByteBuffer)
          digest
      }
      .map(d => ByteString(d.digest()))
}
