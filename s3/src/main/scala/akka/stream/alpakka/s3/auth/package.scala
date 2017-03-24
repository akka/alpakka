/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.s3

import java.security.MessageDigest
import javax.xml.bind.DatatypeConverter

import akka.stream.scaladsl.{Flow, Keep, Sink}
import akka.util.ByteString

import scala.concurrent.Future

package object auth {
  def encodeHex(bytes: Array[Byte]): String = DatatypeConverter.printHexBinary(bytes).toLowerCase

  def encodeHex(bytes: ByteString): String = encodeHex(bytes.toArray)

  def digest(algorithm: String = "SHA-256"): Sink[ByteString, Future[ByteString]] =
    Flow[ByteString]
      .fold(MessageDigest.getInstance(algorithm)) {
        case (digest, bytes) =>
          digest.update(bytes.asByteBuffer)
          digest
      }
      .map(d => ByteString(d.digest()))
      .toMat(Sink.head[ByteString])(Keep.right)
}
