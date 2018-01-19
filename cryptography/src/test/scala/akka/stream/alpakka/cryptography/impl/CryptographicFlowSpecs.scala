/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.cryptography.impl.cry

import javax.crypto.{KeyGenerator, SecretKey}

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.{ActorMaterializer, ActorMaterializerSettings, Supervision}
import akka.stream.alpakka.cryptography.impl.CryptographicFlows
import akka.stream.alpakka.cryptography.impl.CryptographicFlows._
import akka.stream.scaladsl.{Sink, Source}
import akka.util.ByteString
import org.scalatest._
import org.scalatest.concurrent.ScalaFutures

class CryptographicFlowSpecs extends WordSpec with Matchers with ScalaFutures {
  "Symmetric Encryption flows" should {
    "Be able to encrypt and decrypt bytestrings" in {

      implicit val as: ActorSystem = ActorSystem()
      implicit val actorMaterializer: ActorMaterializer = ActorMaterializer {
        val decider: Supervision.Decider = error => {
          println(s"Error encountered: $error")
          Supervision.Stop
        }
        ActorMaterializerSettings(as).withSupervisionStrategy(decider)
      }

      val keyGenerator = KeyGenerator.getInstance("AES")

      val key = keyGenerator.generateKey()

      val toEncrypt = "some"

      val src: Source[ByteString, NotUsed] = Source(List(ByteString(toEncrypt)))

      val res = src.map(symmetricEncryption(key)).map(symmetricDecryption(key)).runWith(Sink.head)


      whenReady(res){s => s.utf8String shouldBe toEncrypt

      }
    }
  }
}
