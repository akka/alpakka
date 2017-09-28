/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.oracle.bmcs

import java.security.MessageDigest
import java.util.Base64

import akka.stream.scaladsl.{Flow, Keep, Sink}
import akka.util.ByteString

import scala.concurrent.Future

package object auth {

  def encodeBase64(bytes: Array[Byte]): String = Base64.getEncoder.encodeToString(bytes)

  def encodeBase64(bytes: ByteString): String = encodeBase64(bytes.toArray)

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
