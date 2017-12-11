/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.mqtt.scaladsl

import akka.Done
import akka.actor.ActorSystem
import akka.stream._
import akka.stream.alpakka.mqtt._
import akka.stream.scaladsl._
import akka.testkit.TestKit
import akka.util.ByteString
import org.eclipse.paho.client.mqttv3.MqttException
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence
import org.scalatest._
import org.scalatest.concurrent.ScalaFutures

import scala.collection.immutable.Seq
import scala.concurrent.Await
import scala.concurrent.duration._

class MqttSourceSpec
    extends TestKit(ActorSystem("MqttSinkSpec"))
    with WordSpecLike
    with Matchers
    with BeforeAndAfterAll
    with ScalaFutures {

  val timeout = 5 seconds
  implicit val defaultPatience =
    PatienceConfig(timeout = 5.seconds, interval = 100.millis)

  implicit val mat = ActorMaterializer()

  //#create-connection-settings
  val connectionSettings = MqttConnectionSettings(
    "tcp://localhost:1883",
    "test-scala-client",
    new MemoryPersistence
  )
  //#create-connection-settings

  val topic1 = "source-spec/topic1"
  val topic2 = "source-spec/topic2"
  val secureTopic = "source-spec/secure-topic1"
  val willTopic = "source-spec/will"

  val sourceSettings = connectionSettings.withClientId(clientId = "source-spec/source")
  val sinkSettings = connectionSettings.withClientId(clientId = "source-spec/sink")

  override def afterAll() = TestKit.shutdownActorSystem(system)

  "mqtt source" should {
    "consume unacknowledged messages from previous sessions using manualAck" in {
      import system.dispatcher

      val topic = "source-spec/manualacks"
      val input = Vector("one", "two", "three", "four", "five")

      //#create-source-with-manualacks
      val connectionSettings = sourceSettings.withCleanSession(false)
      val mqttSourceSettings = MqttSourceSettings(connectionSettings, Map(topic -> MqttQoS.AtLeastOnce))
      val mqttSource = MqttSource.atLeastOnce(mqttSourceSettings, 8)
      //#create-source-with-manualacks

      val (subscribed, unackedResult) = mqttSource.take(input.size).toMat(Sink.seq)(Keep.both).run()
      val mqttSink = MqttSink(sinkSettings, MqttQoS.AtLeastOnce)

      Await.ready(subscribed, timeout)
      Source(input).map(item => MqttMessage(topic, ByteString(item))).runWith(mqttSink)

      unackedResult.futureValue.map(message => message.message.payload.utf8String) should equal(input)

      //#run-source-with-manualacks
      val result = mqttSource
        .mapAsync(1)(cm => cm.messageArrivedComplete().map(_ => cm.message))
        .take(input.size)
        .runWith(Sink.seq)
      //#run-source-with-manualacks
      result.futureValue.map(message => message.payload.utf8String) should equal(input)
    }

    "keep connection open if downstream closes and there are pending acks" in {
      val topic = "source-spec/pendingacks"
      val input = Vector("one", "two", "three", "four", "five")

      //#create-source-with-manualacks
      val connectionSettings = sourceSettings.withCleanSession(false)
      val mqttSourceSettings = MqttSourceSettings(connectionSettings, Map(topic -> MqttQoS.AtLeastOnce))
      val mqttSource = MqttSource.atLeastOnce(mqttSourceSettings, 8)
      //#create-source-with-manualacks

      val (subscribed, unackedResult) = mqttSource.take(input.size).toMat(Sink.seq)(Keep.both).run()
      val mqttSink = MqttSink(sinkSettings, MqttQoS.AtLeastOnce)

      Await.ready(subscribed, timeout)
      Source(input).map(item => MqttMessage(topic, ByteString(item))).runWith(mqttSink).futureValue shouldBe Done

      unackedResult.futureValue.map(cm => {
        noException should be thrownBy cm.messageArrivedComplete().futureValue
      })
    }

    "receive a message from a topic" in {
      val msg = MqttMessage(topic1, ByteString("ohi"))

      val settings = MqttSourceSettings(sourceSettings, Map(topic1 -> MqttQoS.AtLeastOnce))
      val (subscribed, result) = MqttSource
        .atMostOnce(settings, 8)
        .toMat(Sink.head)(Keep.both)
        .run()

      Await.ready(subscribed, timeout)
      Source.single(msg).runWith(MqttSink(sinkSettings, MqttQoS.AtLeastOnce))

      result.futureValue shouldBe msg
    }

    "receive messages from multiple topics" in {
      val messages = (0 until 7)
        .flatMap(
          i =>
            Seq(
              MqttMessage(topic1, ByteString(s"ohi_$i")),
              MqttMessage(topic2, ByteString(s"ohi_$i"))
          )
        )

      //#create-source
      val settings = MqttSourceSettings(
        sourceSettings,
        Map(topic1 -> MqttQoS.AtLeastOnce, topic2 -> MqttQoS.AtLeastOnce)
      )

      val mqttSource = MqttSource.atMostOnce(settings, bufferSize = 8)
      //#create-source

      //#run-source
      val (subscribed, result) = mqttSource
        .take(messages.size)
        .toMat(Sink.seq)(Keep.both)
        .run()
      //#run-source

      Await.ready(subscribed, timeout)
      //#run-sink
      Source(messages).runWith(MqttSink(connectionSettings, MqttQoS.AtLeastOnce))
      //#run-sink

      result.futureValue shouldBe messages
    }

    "fail connection when not providing the requested credentials" in {
      val settings =
        MqttSourceSettings(sourceSettings.withAuth("username1", "bad_password"),
                           Map(secureTopic -> MqttQoS.AtLeastOnce))

      val first = MqttSource.atMostOnce(settings, 8).runWith(Sink.head)

      whenReady(first.failed) {
        case e: MqttException => e.getMessage should be("Not authorized to connect")
        case e => throw e
      }
    }

    "receive a message from a topic with right credentials" in {
      val msg = MqttMessage(secureTopic, ByteString("ohi"))

      val settings = MqttSourceSettings(sourceSettings
                                          .withAuth("username1", "password1"),
                                        Map(secureTopic -> MqttQoS.AtLeastOnce))
      val (subscribed, result) = MqttSource
        .atMostOnce(settings, 8)
        .toMat(Sink.head)(Keep.both)
        .run()

      Await.ready(subscribed, timeout)
      Source.single(msg).runWith(MqttSink(sinkSettings.withAuth("username1", "password1"), MqttQoS.AtLeastOnce))

      result.futureValue shouldBe msg
    }

    "signal backpressure" in {
      val bufferSize = 8
      val overflow = 4
      val messages = (1 until bufferSize + overflow)
        .map(i => s"ohi_$i")

      val settings = MqttSourceSettings(sourceSettings, Map(topic1 -> MqttQoS.AtLeastOnce))
      val (subscribed, result) = MqttSource
        .atMostOnce(settings, bufferSize)
        .take(messages.size)
        .toMat(Sink.seq)(Keep.both)
        .run()

      Await.ready(subscribed, timeout)
      Source(messages)
        .map(m => MqttMessage(topic1, ByteString(m)))
        .runWith(MqttSink(sinkSettings, MqttQoS.AtLeastOnce))

      result.futureValue.map(m => m.payload.utf8String) shouldBe messages
    }

    "work with fast downstream" in {
      val bufferSize = 8
      val overflow = 4
      val messages = (1 until bufferSize + overflow)
        .map(i => s"ohi_$i")

      val settings = MqttSourceSettings(sourceSettings, Map(topic1 -> MqttQoS.AtLeastOnce))
      val (subscribed, result) = MqttSource
        .atMostOnce(settings, bufferSize)
        .take(messages.size)
        .toMat(Sink.seq)(Keep.both)
        .run()

      Await.ready(subscribed, timeout)
      Source(messages)
        .map(m => MqttMessage(topic1, ByteString(m)))
        .runWith(MqttSink(sinkSettings, MqttQoS.AtLeastOnce))

      result.futureValue.map(m => m.payload.utf8String) shouldBe messages
    }

    "support multiple materialization" in {
      val settings = MqttSourceSettings(sourceSettings, Map(topic1 -> MqttQoS.AtLeastOnce))
      val source = MqttSource.atMostOnce(settings, 8)

      val (subscribed, elem) = source.toMat(Sink.head)(Keep.both).run()

      Await.ready(subscribed, timeout)
      Source.single(MqttMessage(topic1, ByteString("ohi"))).runWith(MqttSink(sinkSettings, MqttQoS.atLeastOnce))
      elem.futureValue shouldBe MqttMessage(topic1, ByteString("ohi"))

      val (subscribed2, elem2) = source.toMat(Sink.head)(Keep.both).run()

      Await.ready(subscribed2, timeout)
      Source.single(MqttMessage(topic1, ByteString("ohi"))).runWith(MqttSink(sinkSettings, MqttQoS.atLeastOnce))
      elem2.futureValue shouldBe MqttMessage(topic1, ByteString("ohi"))
    }

    "support will message" ignore {
      import system.dispatcher

      val (binding, connection) = Tcp().bind("localhost", 1337).toMat(Sink.head)(Keep.both).run()

      val ks = connection.map(
        _.handleWith(
          Tcp()
            .outgoingConnection("localhost", 1883)
            .viaMat(KillSwitches.single)(Keep.right)
        )
      )

      whenReady(binding) { _ =>
        val settings = MqttSourceSettings(
          sourceSettings
            .withClientId("source-spec/testator")
            .withBroker("tcp://localhost:1337")
            .withWill(Will(MqttMessage(willTopic, ByteString("ohi")), MqttQoS.AtLeastOnce, retained = true)),
          Map(willTopic -> MqttQoS.AtLeastOnce)
        )
        val source = MqttSource.atMostOnce(settings, 8)

        val sub = source.runWith(Sink.head)
        whenReady(sub) { _ =>
          whenReady(ks)(_.shutdown())
        }
      }

      {
        val settings =
          MqttSourceSettings(sourceSettings.withClientId("source-spec/executor"),
                             Map(willTopic -> MqttQoS.AtLeastOnce))
        val source = MqttSource.atMostOnce(settings, 8)

        val (subscribed, elem) = source.toMat(Sink.head)(Keep.both).run()

        Await.ready(subscribed, timeout)
        elem.futureValue shouldBe MqttMessage(willTopic, ByteString("ohi"))
      }
    }
  }
}
