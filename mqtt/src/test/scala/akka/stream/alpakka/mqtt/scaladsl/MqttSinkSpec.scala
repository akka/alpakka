/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.mqtt.scaladsl

import akka.Done
import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.alpakka.mqtt.{MqttSourceSettings, _}
import akka.stream.scaladsl.{Sink, Source}
import akka.testkit.TestKit
import akka.util.ByteString
import org.eclipse.paho.client.mqttv3.MqttSecurityException
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence
import org.scalatest.concurrent.ScalaFutures
import org.scalatest._

import scala.concurrent.duration._

class MqttSinkSpec
    extends TestKit(ActorSystem("MqttSinkSpec"))
    with WordSpecLike
    with Matchers
    with BeforeAndAfterAll
    with ScalaFutures {

  implicit val defaultPatience =
    PatienceConfig(timeout = 5.seconds, interval = 100.millis)

  implicit val mat = ActorMaterializer()
  val connectionSettings = MqttConnectionSettings(
    "tcp://localhost:1883",
    "test-client",
    new MemoryPersistence
  )

  val topic = "sink-spec/topic1"
  val secureTopic = "sink-spec/secure-topic1"

  val sourceSettings = connectionSettings.withClientId(clientId = "sink-spec/source")
  val sinkSettings = connectionSettings.withClientId(clientId = "sink-spec/sink")

  override def afterAll() = TestKit.shutdownActorSystem(system)

  "mqtt sink" should {
    "send one message to a topic" in {
      println("TES2")
      val msg = MqttMessage(topic, ByteString("ohi"))

      Source.single(msg).runWith(MqttSink(sinkSettings, MqttQoS.atLeastOnce))

      val mqttSettings = MqttSourceSettings(sourceSettings, Map(topic -> MqttQoS.atLeastOnce))
      val message = MqttSource
        .atMostOnce(mqttSettings, 8)
        .runWith(Sink.head)

      message.futureValue shouldBe msg
      println("TES2FIN")
    }

    "send multiple messages to a topic" in {
      val msg = MqttMessage(topic, ByteString("ohi"))
      val numOfMessages = 42

      val messagesFuture =
        MqttSource
          .atMostOnce(MqttSourceSettings(sourceSettings, Map(topic -> MqttQoS.atLeastOnce)), 8)
          .take(numOfMessages)
          .runWith(Sink.seq)

      Source(1 to numOfMessages).map(_ => msg).runWith(MqttSink(sinkSettings, MqttQoS.atLeastOnce))

      val messages = messagesFuture.futureValue
      messages should have length numOfMessages
      messages foreach { _ shouldBe msg }
    }

    "fail to publish when credentials are not provided" in {
      val msg = MqttMessage(secureTopic, ByteString("ohi"))

      val termination =
        Source.single(msg).runWith(MqttSink(sinkSettings.withAuth("username1", "bad_password"), MqttQoS.atLeastOnce))

      whenReady(termination.failed) { ex =>
        ex shouldBe an[MqttSecurityException]
        ex.getMessage should include("Not authorized to connect")
      }
    }

    "publish when credentials are provided" in {
      val msg = MqttMessage(secureTopic, ByteString("ohi"))

      val termination = Source
        .single(msg)
        .runWith(MqttSink(sinkSettings.withAuth("username1", "password1"), MqttQoS.atLeastOnce))

      termination.futureValue shouldBe Done
    }
  }
}
