/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.cryptography.scaladsl

import java.security.{KeyPair, KeyPairGenerator}
import javax.crypto.{KeyGenerator, SecretKey}

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.alpakka.cryptography.scaladsl.CryptographicFlows._
import akka.stream.scaladsl.{Sink, Source}
import akka.util.ByteString
import org.scalacheck.{Arbitrary, Gen}
import org.scalatest._
import org.scalatest.concurrent.{IntegrationPatience, ScalaFutures}
import org.scalatest.prop.PropertyChecks
import scala.concurrent.ExecutionContext.Implicits.global

import scala.concurrent.Future

class CryptographicFlowSpec
    extends WordSpec
    with Matchers
    with ScalaFutures
    with PropertyChecks
    with IntegrationPatience {

  implicit val as: ActorSystem = ActorSystem()
  implicit val actorMaterializer: ActorMaterializer = ActorMaterializer()

  "CryptographicFlow" can {
    "Symmetric Encryption flows" should {
      "Be able to encrypt and decrypt bytestrings" in {
        forAll(minSuccessful(50), sizeRange(20)) { (key: SecretKey, toEncrypt: List[String]) =>
          val src: Source[ByteString, NotUsed] = Source(toEncrypt.map(ByteString.apply))

          val res: Future[ByteString] = src
            .via(symmetricEncryption(key))
            .via(symmetricDecryption(key))
            .runWith(Sink.fold(ByteString.empty)(_ concat _))

          whenReady(res) { s =>
            s.utf8String shouldBe toEncrypt.mkString("")
          }
        }
      }

      "Be able to handle bytestring chunking" in {
        val keyGenerator = KeyGenerator.getInstance("AES")
        val secretKey = keyGenerator.generateKey()

        val byteString = ByteString("byte by byte")

        val res = Source
          .single(byteString)
          .mapConcat(bs => bs.toArray.toList)
          .map(b => ByteString(b))
          .via(symmetricEncryption(secretKey))
          .runWith(Sink.fold(ByteString.empty)(_ concat _))

        val decrypted: Future[ByteString] = res.flatMap { encrypted =>
          Source
            .single(encrypted)
            .via(symmetricDecryption(secretKey))
            .runWith(Sink.fold(ByteString.empty)(_ concat _))
        }

        whenReady(decrypted) {
          _ shouldBe byteString
        }
      }
    }

    "Asymmetric Encryption flows" should {
      "Be able to encrypt and decrypt bytestrings" in {
        forAll(minSuccessful(10), sizeRange(5)) { (keyPair: KeyPair, toEncrypt: List[String]) =>
          val src: Source[ByteString, NotUsed] = Source(toEncrypt.map(ByteString.apply))

          val res: Future[ByteString] = src
            .via(asymmetricEncryption(keyPair.getPublic))
            .via(asymmetricDecryption(keyPair.getPrivate))
            .runWith(Sink.fold(ByteString.empty)(_ concat _))

          whenReady(res) { s =>
            s.utf8String shouldBe toEncrypt.mkString("")
          }
        }
      }
    }

    "Be able to handle bytestring chunking" in {
      val keyPairGenerator = KeyPairGenerator.getInstance("RSA")
      val keyPair = keyPairGenerator.generateKeyPair()

      val byteString = ByteString("byte by byte")

      val res = Source
        .single(byteString)
        .mapConcat(bs => bs.toArray.toList)
        .map(b => ByteString(b))
        .via(asymmetricEncryption(keyPair.getPublic))
        .runWith(Sink.fold(ByteString.empty)(_ concat _))

      val decrypted: Future[ByteString] = res.flatMap { encrypted =>
        Source
          .single(encrypted)
          .via(asymmetricDecryption(keyPair.getPrivate))
          .runWith(Sink.fold(ByteString.empty)(_ concat _))
      }

      whenReady(decrypted) {
        _ shouldBe byteString
      }
    }
  }

  implicit def arbitrarySecretKey: Arbitrary[SecretKey] = Arbitrary {
    Gen.resultOf((_: Unit) => {
      val keyGenerator = KeyGenerator.getInstance("AES")
      keyGenerator.generateKey()
    })
  }

  val keyPairGenerator = KeyPairGenerator.getInstance("RSA")
  keyPairGenerator.initialize(1024)
  implicit def arbitraryKeyPair: Arbitrary[KeyPair] = Arbitrary {
    Gen.resultOf((_: Unit) => {
      keyPairGenerator.generateKeyPair()
    })
  }

}
