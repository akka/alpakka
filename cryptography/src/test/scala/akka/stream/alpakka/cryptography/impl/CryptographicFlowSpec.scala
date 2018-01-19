/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.cryptography.impl

import javax.crypto.{KeyGenerator, SecretKey}

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.alpakka.cryptography.impl.CryptographicFlows._
import akka.stream.scaladsl.{Sink, Source}
import akka.stream.{ActorMaterializer, ActorMaterializerSettings, Supervision}
import akka.util.ByteString
import org.scalacheck.{Arbitrary, Gen}
import org.scalatest._
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.prop.PropertyChecks

import scala.concurrent.Future

class CryptographicFlowSpec extends WordSpec with Matchers with ScalaFutures with PropertyChecks {

  implicit val as: ActorSystem = ActorSystem()
  implicit val actorMaterializer: ActorMaterializer = ActorMaterializer()

  "Symmetric Encryption flows" should {
    "Be able to encrypt and decrypt bytestrings" in {
      forAll(minSuccessful(1000)) { (key: SecretKey, toEncrypt: List[String]) =>
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
  }

  implicit def arbitrarySecretKey: Arbitrary[SecretKey] = Arbitrary {
    Gen.resultOf((_: Unit) => {
      val keyGenerator = KeyGenerator.getInstance("AES")

      keyGenerator.generateKey()
    })
  }

}
