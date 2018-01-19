package akka.stream.alpakka.cryptography.Example

import java.security.KeyPairGenerator
import javax.crypto.{KeyGenerator}

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.alpakka.cryptography.impl.CryptographicFlows._
import akka.stream.scaladsl.{Sink, Source}
import akka.util.ByteString
import org.scalatest.{Matchers, WordSpec}
import org.scalatest.concurrent.ScalaFutures

import scala.concurrent.Future

class ExampleSpec extends WordSpec with Matchers with ScalaFutures {

  "Cryptography" should {
    "Provide a symmetric scala example" in {
      //#init-client

      implicit val actorSystem = ActorSystem()
      implicit val materialize = ActorMaterializer()

      //#init-client

      //#scala-symmetric
      val keyGenerator = KeyGenerator.getInstance("AES")

      val randomKey = keyGenerator.generateKey()

      val toEncrypt = List("Some", "string", "for ", "you")

      val sourceOfUnencryptedData: Source[ByteString, NotUsed] = Source(toEncrypt.map(ByteString.apply))

      val sourceOfEncryptedData: Source[ByteString, NotUsed] = sourceOfUnencryptedData
        .via(symmetricEncryption(randomKey))

      val sourceOfDecryptedData: Source[ByteString, NotUsed] = sourceOfEncryptedData.via(symmetricDecryption(randomKey))


      val resultOfDecryption: Future[ByteString] = sourceOfDecryptedData.runWith(Sink.fold(ByteString.empty)(_ concat _))

      whenReady(resultOfDecryption){r =>
        r.utf8String shouldBe toEncrypt.mkString("")
      }
      //#scala-symmetric
    }

    "Provide an asymmetric example" in {

      implicit val actorSystem = ActorSystem()
      implicit val materialize = ActorMaterializer()

      //#scala-asymmetric

      val keyPairGenerator = KeyPairGenerator.getInstance("RSA")
      val generatedKeyPair = keyPairGenerator.generateKeyPair()

      val toEncrypt = List("Some", "string", "for ", "you")

      val sourceOfUnencryptedData: Source[ByteString, NotUsed] = Source(toEncrypt.map(ByteString.apply))

      val sourceOfEncryptedData: Source[ByteString, NotUsed] = sourceOfUnencryptedData
        .via(asymmetricEncryption(generatedKeyPair.getPublic))

      val sourceOfDecryptedData: Source[ByteString, NotUsed] = sourceOfEncryptedData.via(asymmetricDecryption(generatedKeyPair.getPrivate))

      val resultOfDecryption: Future[ByteString] = sourceOfDecryptedData.runWith(Sink.fold(ByteString.empty)(_ concat _))

      whenReady(resultOfDecryption){r =>
        r.utf8String shouldBe toEncrypt.mkString("")
      }
      //#scala-asymmetric
    }
  }
}
