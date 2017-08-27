/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.mqtt.scaladsl

import akka.actor.ActorSystem
import akka.stream._
import akka.stream.alpakka.mqtt._
import akka.stream.scaladsl._
import akka.stream.testkit.scaladsl.TestSink
import akka.testkit.TestKit
import akka.util.ByteString
import org.eclipse.paho.client.mqttv3.MqttException
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence
import org.scalatest._
import org.scalatest.concurrent.ScalaFutures

import scala.collection.immutable.Seq
import scala.concurrent.duration._

class MqttSourceSpec
    extends TestKit(ActorSystem("MqttSinkSpec"))
    with WordSpecLike
    with Matchers
    with BeforeAndAfterAll
    with ScalaFutures {

  implicit val defaultPatience =
    PatienceConfig(timeout = 10.seconds, interval = 100.millis)

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

  "mqtt source" should {
    "receive a message from a topic" in {
      val settings = MqttSourceSettings(sourceSettings, Map(topic1 -> MqttQoS.AtLeastOnce))
      val (subscriptionFuture, probe) = MqttSource(settings, 8).toMat(TestSink.probe)(Keep.both).run()
      whenReady(subscriptionFuture) { _ =>
        val msg = MqttMessage(topic1, ByteString("ohi"))
        Source.single(msg).runWith(MqttSink(sinkSettings, MqttQoS.AtLeastOnce))
        probe.requestNext shouldBe msg
      }
    }

    "receive messages from multiple topics" in {
      val messageCount = 7

      //#create-source
      val settings = MqttSourceSettings(
        connectionSettings.withClientId("source-spec/source"),
        Map(topic1 -> MqttQoS.AtLeastOnce, topic2 -> MqttQoS.AtLeastOnce)
      )

      val mqttSource = MqttSource(settings, bufferSize = 8)
      //#create-source

      //#run-source
      val (subscriptionFuture, result) = mqttSource
        .map(m => s"${m.topic}_${m.payload.utf8String}")
        .take(messageCount * 2)
        .toMat(Sink.seq)(Keep.both)
        .run()
      //#run-source

      whenReady(subscriptionFuture) { _ =>
        val expected = (0 until messageCount).flatMap { i =>
          Seq(s"${topic1}_$i", s"${topic2}_$i")
        }

        val messages = (0 until messageCount).flatMap { i =>
          Seq(
            MqttMessage(topic1, ByteString(i.toString)),
            MqttMessage(topic2, ByteString(i.toString))
          )
        }

        //#run-sink
        Source(messages).runWith(MqttSink(connectionSettings.withClientId("source-spec/sink"), MqttQoS.AtLeastOnce))
        //#run-sink
        result.futureValue shouldBe expected
      }
    }

    "fail connection when not providing the requested credentials" in {
      val settings =
        MqttSourceSettings(sourceSettings.withAuth("username1", "bad_password"),
                           Map(secureTopic -> MqttQoS.AtLeastOnce))
      val first = MqttSource(settings, 8).runWith(Sink.head)
      whenReady(first.failed) {
        case e: MqttException => e.getMessage should be("Not authorized to connect")
        case e => throw e
      }
    }

    "receive a message from a topic with right credentials" in {
      val settings =
        MqttSourceSettings(sourceSettings.withAuth("username1", "password1"), Map(secureTopic -> MqttQoS.AtLeastOnce))
      val (subscriptionFuture, probe) = MqttSource(settings, 8).toMat(TestSink.probe)(Keep.both).run()
      whenReady(subscriptionFuture) { _ =>
        val msg = MqttMessage(secureTopic, ByteString("ohi"))
        Source.single(msg).runWith(MqttSink(sinkSettings.withAuth("username1", "password1"), MqttQoS.AtLeastOnce))
        probe.requestNext shouldBe msg
      }
    }

    "signal backpressure" in {
      val bufferSize = 8
      val overflow = 4

      val settings = MqttSourceSettings(sourceSettings, Map(topic1 -> MqttQoS.AtLeastOnce))
      val (subscriptionFuture, probe) = MqttSource(settings, bufferSize).toMat(TestSink.probe)(Keep.both).run()
      whenReady(subscriptionFuture) { _ =>
        Source(1 to bufferSize + overflow)
          .map { i =>
            MqttMessage(topic1, ByteString(s"ohi_$i"))
          }
          .runWith(MqttSink(sinkSettings, MqttQoS.AtLeastOnce))

        (1 to bufferSize + overflow) foreach { i =>
          probe.requestNext shouldBe MqttMessage(topic1, ByteString(s"ohi_$i"))
        }
      }
    }

    "work with fast downstream" in {
      val bufferSize = 8
      val overflow = 4

      val settings = MqttSourceSettings(sourceSettings, Map(topic1 -> MqttQoS.AtLeastOnce))
      val (subscriptionFuture, probe) = MqttSource(settings, bufferSize).toMat(TestSink.probe)(Keep.both).run()
      whenReady(subscriptionFuture) { _ =>
        probe.request((bufferSize + overflow).toLong)

        Source(1 to bufferSize + overflow)
          .map { i =>
            MqttMessage(topic1, ByteString(s"ohi_$i"))
          }
          .runWith(MqttSink(sinkSettings, MqttQoS.AtLeastOnce))

        (1 to bufferSize + overflow) foreach { i =>
          probe.expectNext() shouldBe MqttMessage(topic1, ByteString(s"ohi_$i"))
        }
      }
    }

    "support multiple materialization" in {
      val settings = MqttSourceSettings(sourceSettings, Map(topic1 -> MqttQoS.AtLeastOnce))
      val source = MqttSource(settings, 8)

      val (sub, elem) = source.toMat(Sink.head)(Keep.both).run()
      whenReady(sub) { _ =>
        Source.single(MqttMessage(topic1, ByteString("ohi"))).runWith(MqttSink(sinkSettings, MqttQoS.atLeastOnce))
        elem.futureValue shouldBe MqttMessage(topic1, ByteString("ohi"))
      }

      val (sub2, elem2) = source.toMat(Sink.head)(Keep.both).run()
      whenReady(sub2) { _ =>
        Source.single(MqttMessage(topic1, ByteString("ohi"))).runWith(MqttSink(sinkSettings, MqttQoS.atLeastOnce))
        elem2.futureValue shouldBe MqttMessage(topic1, ByteString("ohi"))
      }
    }

    "support will message" in {
      import system.dispatcher

      val (binding, connection) = Tcp().bind("localhost", 1337).toMat(Sink.head)(Keep.both).run()

      val ks = connection.map(
        _.handleWith(Tcp().outgoingConnection("localhost", 1883).viaMat(KillSwitches.single)(Keep.right))
      )

      whenReady(binding) { _ =>
        val settings = MqttSourceSettings(
          sourceSettings
            .withClientId("source-spec/testator")
            .withBroker("tcp://localhost:1337")
            .withWill(Will(MqttMessage(willTopic, ByteString("ohi")), MqttQoS.AtLeastOnce, retained = true)),
          Map(willTopic -> MqttQoS.AtLeastOnce)
        )
        val source = MqttSource(settings, 8)

        val sub = source.toMat(Sink.head)(Keep.left).run()
        whenReady(sub) { _ =>
          whenReady(ks)(_.shutdown())
        }
      }

      {
        val settings =
          MqttSourceSettings(sourceSettings.withClientId("source-spec/executor"),
                             Map(willTopic -> MqttQoS.AtLeastOnce))
        val source = MqttSource(settings, 8)

        val (sub, elem) = source.toMat(Sink.head)(Keep.both).run()
        whenReady(sub) { _ =>
          elem.futureValue shouldBe MqttMessage(willTopic, ByteString("ohi"))
        }
      }
    }
  }
}
