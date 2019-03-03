/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package docs.scaladsl

import akka.Done
import akka.actor.ActorSystem
import akka.stream.alpakka.mqtt
import akka.stream.{ActorMaterializer, Materializer}
import akka.stream.alpakka.mqtt.scaladsl.{MqttSink, MqttSource}
import akka.stream.alpakka.mqtt._
import akka.stream.scaladsl.{Keep, Sink, Source}
import akka.testkit.TestKit
import akka.util.ByteString
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence
import org.eclipse.paho.client.mqttv3.{MqttException, MqttSecurityException}
import org.scalatest._
import org.scalatest.concurrent.ScalaFutures

import scala.concurrent.Await
import scala.concurrent.duration._

class MqttSinkSpec
    extends TestKit(ActorSystem("MqttSinkSpec"))
    with WordSpecLike
    with Matchers
    with BeforeAndAfterAll
    with ScalaFutures {

  val timeout = 5.seconds
  implicit val defaultPatience =
    PatienceConfig(timeout = 5.seconds, interval = 100.millis)

  implicit val mat: Materializer = ActorMaterializer()
  val connectionSettings = MqttConnectionSettings(
    "tcp://localhost:1883",
    "test-client",
    new MemoryPersistence
  )

  val topic = "sink-spec/topic1"
  val topic2 = "sink-spec/topic2"
  val secureTopic = "sink-spec/secure-topic1"

  val sinkSettings = connectionSettings.withClientId(clientId = "sink-spec/sink")

  override def afterAll() = TestKit.shutdownActorSystem(system)

  "mqtt sink" should {
    "send one message to a topic" in {
      val msg = MqttMessage(topic, ByteString("ohi"))

      val (subscribed, message) = MqttSource
        .atMostOnce(connectionSettings.withClientId(clientId = "sink-spec/source"),
                    MqttSubscriptions(topic, MqttQoS.AtLeastOnce),
                    8)
        .toMat(Sink.head)(Keep.both)
        .run()

      Await.ready(subscribed, timeout)
      Source.single(msg).runWith(MqttSink(sinkSettings, MqttQoS.atLeastOnce))

      message.futureValue shouldBe msg
    }

    "send multiple messages to a topic" in {
      val msg = MqttMessage(topic2, ByteString("ohi"))
      val numOfMessages = 5

      val (subscribed, messagesFuture) =
        MqttSource
          .atMostOnce(connectionSettings.withClientId(clientId = "sink-spec/source"),
                      mqtt.MqttSubscriptions(topic2, MqttQoS.atLeastOnce),
                      8)
          .take(numOfMessages)
          .toMat(Sink.seq)(Keep.both)
          .run()

      Await.ready(subscribed, timeout)
      Source(1 to numOfMessages).map(_ => msg).runWith(MqttSink(sinkSettings, MqttQoS.atLeastOnce))

      val messages = messagesFuture.futureValue
      messages should have length numOfMessages
      messages foreach { _ shouldBe msg }
    }

    "connection should fail to wrong broker" in {
      val wrongConnectionSettings = sinkSettings.withBroker("tcp://localhost:1884")
      val msg = MqttMessage(secureTopic, ByteString("ohi"))

      val termination = Source
        .single(msg)
        .runWith(MqttSink(wrongConnectionSettings, MqttQoS.atLeastOnce))

      termination.failed.futureValue shouldBe an[MqttException]
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

    "received retained message on new client" in {
      val msg = MqttMessage(topic, ByteString("ohi")).withQos(MqttQoS.atLeastOnce).withRetained(true)

      val messageSent = Source.single(msg).runWith(MqttSink(sinkSettings, MqttQoS.atLeastOnce))

      Await.ready(messageSent, 3.seconds)

      val messageFuture =
        MqttSource
          .atMostOnce(connectionSettings.withClientId("source-spec/retained"),
                      mqtt.MqttSubscriptions(topic, MqttQoS.atLeastOnce),
                      8)
          .runWith(Sink.head)

      val message = messageFuture.futureValue
      message.topic shouldBe msg.topic
      message.payload shouldBe msg.payload
    }
  }
}
