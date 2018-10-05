package akka.stream.alpakka.cryptography.impl
import java.io.ByteArrayOutputStream

import akka.NotUsed
import akka.stream.scaladsl.Flow
import akka.util.ByteString
import javax.crypto.{Cipher, CipherOutputStream}

object CipherFlow {
  def cipherFlow(cipher: Cipher): Flow[ByteString, ByteString, NotUsed] =
    Flow[ByteString]
      .map { bytes =>
        val processed = new ByteArrayOutputStream(16)
        val cipherOutputStream = new CipherOutputStream(processed, cipher)
        cipherOutputStream.write(bytes.toArray)
        cipherOutputStream.close()

        ByteString(processed.toByteArray)
      }
}
