/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.cryptography.impl

import javax.crypto.KeyGenerator

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.alpakka.cryptography.impl.CryptographicFlows._
import akka.stream.scaladsl.{Sink, Source}
import akka.stream.{ActorMaterializer, ActorMaterializerSettings, Supervision}
import akka.util.ByteString
import org.scalatest._
import org.scalatest.concurrent.ScalaFutures

import scala.concurrent.Future

class CryptographicFlowSpec extends WordSpec with Matchers with ScalaFutures {
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

      val toEncrypt = List("some", "strings", "to", "encrypt")

      val src: Source[ByteString, NotUsed] = Source(toEncrypt.map(ByteString.apply))

      val res: Future[ByteString] = src
        .via(symmetricEncryptionFlow(key))
        .map(symmetricDecryption(key)).runWith(Sink.fold(ByteString.empty)(_ concat _))


      whenReady(res){s =>
        s.utf8String shouldBe toEncrypt.mkString("")
        println(s.utf8String)

      }
    }
  }
}
