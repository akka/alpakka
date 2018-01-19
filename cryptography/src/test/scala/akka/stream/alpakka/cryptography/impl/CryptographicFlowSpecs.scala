package akka.stream.alpakka.cryptography.impl

import javax.crypto.SecretKey

import org.scalatest._

class CryptographicFlowSpec extends WordSpec with Matchers {
  "Symmetric Encryption flows" should {
    "Be able to encrypt and decrypt bytestrings" in {
      val key = new SecretKey
    }
  }
}
