/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.cryptography.impl

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Sink, Source}
import akka.util.ByteString
import javax.crypto.{Cipher, KeyGenerator, SecretKey}
import org.scalacheck.{Arbitrary, Gen}
import akka.stream.alpakka.cryptography.scaladsl.CryptographicFlows._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import org.scalatest._
import org.scalatest.concurrent.{IntegrationPatience, ScalaFutures}
import org.scalatest.prop.PropertyChecks

class CryptographicFlowSpec
    extends WordSpec
    with Matchers
    with ScalaFutures
    with PropertyChecks
    with IntegrationPatience {

  implicit val as: ActorSystem = ActorSystem()
  implicit val actorMaterializer: ActorMaterializer = ActorMaterializer()

  "Flows" can {
    "Encryption flows" should {
      "Be able to encrypt and decrypt bytestrings" in {
//        forAll(minSuccessful(50), sizeRange(20)) { (key: SecretKey, toEncrypt: List[String]) =>
        val toEncrypt = List("Hello crypto", "!")
        toEncrypt.map(_.getBytes)
        val key = KeyGenerator.getInstance("AES").generateKey()
          val src: Source[ByteString, NotUsed] = Source(toEncrypt.map(ByteString.apply))
          val encryptionCipher = Cipher.getInstance("AES/ECB/PKCS5Padding")
          encryptionCipher.init(Cipher.ENCRYPT_MODE, key)

          val decryptionCipher = Cipher.getInstance("AES/ECB/PKCS5Padding")
          decryptionCipher.init(Cipher.DECRYPT_MODE, key)

          val res: Future[ByteString] = src
            .via(cipherFlow(encryptionCipher))
            .via(cipherFlow(decryptionCipher))
            .runWith(Sink.fold(ByteString.empty)(_ concat _))

          whenReady(res) { s =>
            s.utf8String shouldBe toEncrypt.mkString("")
          }

      }
//
//      "Be able to handle bytestring chunking" in {
//        val keyGenerator = KeyGenerator.getInstance("AES")
//        val secretKey = keyGenerator.generateKey()
//
//        val byteString = ByteString("byte by byte")
//
//        val res = Source
//          .single(byteString)
//          .mapConcat(bs => bs.toArray.toList)
//          .map(b => ByteString(b))
//          .via(encrypt(secretKey))
//          .runWith(Sink.fold(ByteString.empty)(_ concat _))
//
//        val decrypted: Future[ByteString] = res.flatMap { encrypted =>
//          Source
//            .single(encrypted)
//            .via(decrypt(secretKey))
//            .runWith(Sink.fold(ByteString.empty)(_ concat _))
//        }
//
//        whenReady(decrypted) {
//          _ shouldBe byteString
//        }
//      }
    }
  }

  implicit def arbitrarySecretKey: Arbitrary[SecretKey] = Arbitrary {
    Gen.resultOf((_: Unit) => {
      val keyGenerator = KeyGenerator.getInstance("AES")
      keyGenerator.generateKey()
    })
  }
//
//  val keyPairGenerator = KeyPairGenerator.getInstance("RSA")
//  keyPairGenerator.initialize(1024)
//  implicit def arbitraryKeyPair: Arbitrary[KeyPair] = Arbitrary {
//    Gen.resultOf((_: Unit) => {
//      keyPairGenerator.generateKeyPair()
//    })
//  }

}
