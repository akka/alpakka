package akka.stream.alpakka.cryptography.impl
import java.io.{ByteArrayInputStream, ByteArrayOutputStream}

import akka.NotUsed
import akka.stream.scaladsl.Flow
import akka.util.ByteString
import javax.crypto.{Cipher, CipherOutputStream}

import scala.collection.mutable.ArrayBuffer

object Flows {
  def encrypt(cipher: Cipher): Flow[ByteString, ByteString, NotUsed] = {
    Flow[ByteString].statefulMapConcat(() => {

      val bufferSize = 16
      val outputStream = new ByteArrayOutputStream(bufferSize)
      val cipherOutputStream = new CipherOutputStream(outputStream, cipher)

      bytes =>
        outputStream.write(bytes.toArray)
        val buffer = Array.ofDim[Byte](bufferSize)
        val inputStream = new ByteArrayInputStream(buffer) // TODO revisit buffer size
        val encryptedBytes = ArrayBuffer.empty[Byte]

        while (inputStream.available() > 0) {
          val numberOfBytesRead = inputStream.read()

          encryptedBytes ++= buffer.take(numberOfBytesRead)
        }

        encryptedBytes.toList.map(bs => ByteString(bs))
    })
  }
}
