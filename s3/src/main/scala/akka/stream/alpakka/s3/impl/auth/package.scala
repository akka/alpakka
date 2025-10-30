/*
 * Copyright (C) since 2016 Lightbend Inc. <https://akka.io>
 */

package akka.stream.alpakka.s3.impl

import java.security.MessageDigest

import akka.NotUsed
import akka.annotation.InternalApi
import akka.stream.scaladsl.Flow
import akka.util.ByteString

package object auth {

  private val Digits = "0123456789abcdef".toCharArray

  @InternalApi private[impl] def encodeHex(bytes: Array[Byte]): String = {
    val length = bytes.length
    val out = new Array[Char](length * 2)
    for (i <- 0 until length) {
      val b = bytes(i)
      out(i * 2) = Digits((b >> 4) & 0xF)
      out(i * 2 + 1) = Digits(b & 0xF)
    }
    new String(out)
  }

  @InternalApi private[impl] def encodeHex(bytes: ByteString): String = encodeHex(bytes.toArray)

  @InternalApi private[impl] def digest(algorithm: String = "SHA-256"): Flow[ByteString, ByteString, NotUsed] =
    Flow[ByteString]
      .fold(MessageDigest.getInstance(algorithm)) {
        case (digest, bytes) =>
          digest.update(bytes.asByteBuffer)
          digest
      }
      .map(d => ByteString(d.digest()))
}
