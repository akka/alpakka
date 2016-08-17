/*
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.contrib.mqtt

import akka.actor.ActorSystem
import akka.stream._
import akka.stream.scaladsl._
import akka.stream.testkit.scaladsl.TestSink
import akka.util.ByteString
import io.moquette.proto.messages.AbstractMessage.QOSType
import io.moquette.proto.messages.PublishMessage
import io.moquette.server.Server
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence
import org.scalatest._
import org.scalatest.concurrent.ScalaFutures
import scala.concurrent._
import scala.concurrent.duration._

import scala.language.reflectiveCalls

class MqttSourceSpec extends WordSpec with Matchers with ScalaFutures {

  import MqttSourceSpec._

  implicit val defaultPatience =
    PatienceConfig(timeout = 5.seconds, interval = 100.millis)

  "mqtt source" should {
    "receive a message from a topic" in withBroker(Map("topic1" -> 0)) { p =>
      val f = fixture(p)
      import f._

      val (subscriptionFuture, probe) = MqttSource(p.settings, 8).toMat(TestSink.probe)(Keep.both).run()
      whenReady(subscriptionFuture) { _ =>
        publish("topic1", "ohi")
        probe.requestNext shouldBe MqttMessage("topic1", ByteString("ohi"))
      }
    }

    "receive messages from multiple topics" in withBroker(Map("topic1" -> 0, "topic2" -> 0)) { p =>
      val f = fixture(p)
      import f._

      val (subscriptionFuture, probe) = MqttSource(p.settings, 8).toMat(TestSink.probe)(Keep.both).run()
      whenReady(subscriptionFuture) { _ =>
        publish("topic1", "ohi")
        publish("topic2", "hello again")
        probe.requestNext shouldBe MqttMessage("topic1", ByteString("ohi"))
        probe.requestNext shouldBe MqttMessage("topic2", ByteString("hello again"))
      }
    }

    "fail stream when disconnected" in withBroker(Map("topic1" -> 0)) { p =>
      val f = fixture(p)
      import f._

      val (subscriptionFuture, probe) = MqttSource(p.settings, 8).toMat(TestSink.probe)(Keep.both).run()
      whenReady(subscriptionFuture) { _ =>
        publish("topic1", "ohi")
        probe.requestNext shouldBe MqttMessage("topic1", ByteString("ohi"))

        server.stopServer()
        probe.expectError.getMessage should be("Connection lost")
      }
    }

    "signal backpressure" in withBroker(Map("topic1" -> 0)) { p =>
      val f = fixture(p)
      import f._

      val bufferSize = 8
      val overflow = 4

      val (subscriptionFuture, probe) = MqttSource(p.settings, bufferSize).toMat(TestSink.probe)(Keep.both).run()
      whenReady(subscriptionFuture) { _ =>

        (1 to bufferSize + overflow) foreach { i =>
          publish("topic1", s"ohi_$i")
        }

        (1 to bufferSize + overflow) foreach { i =>
          probe.requestNext shouldBe MqttMessage("topic1", ByteString(s"ohi_$i"))
        }
      }
    }

    "support multiple materialization" in withBroker(Map("topic1" -> 0)) { p =>
      val f = fixture(p)
      import f._
      import f.system.dispatcher

      val source = MqttSource(p.settings, 8)

      val (sub, elem) = source.toMat(Sink.head)(Keep.both).run()
      whenReady(sub) { _ =>
        publish("topic1", s"ohi")
        elem.futureValue shouldBe MqttMessage("topic1", ByteString("ohi"))
      }

      val (sub2, elem2) = source.toMat(Sink.head)(Keep.both).run()
      whenReady(sub2) { _ =>
        publish("topic1", s"ohi")
        elem2.futureValue shouldBe MqttMessage("topic1", ByteString("ohi"))
      }
    }
  }

  def publish(topic: String, payload: String)(implicit server: Server) = {
    val msg = new PublishMessage()
    msg.setPayload(ByteString(payload).toByteBuffer)
    msg.setTopicName(topic)
    msg.setQos(QOSType.MOST_ONE)
    server.internalPublish(msg)
  }

  def withBroker(subscriptions: Map[String, Int])(test: FixtureParam => Any) = {
    implicit val sys = ActorSystem("MqttSourceSpec")
    val mat = ActorMaterializer()

    val settings = MqttSourceSettings(
      MqttConnectionSettings(
        "tcp://localhost:1883",
        "test-client",
        new MemoryPersistence
      ),
      subscriptions
    )

    val server = new Server()
    server.startServer()
    try {
      test(FixtureParam(settings, server, sys, mat))
    } finally {
      server.stopServer()
    }

    Await.ready(sys.terminate(), 5.seconds)
  }

}

object MqttSourceSpec {

  def fixture(p: FixtureParam) = new {
    implicit val server = p.server
    implicit val system = p.sys
    implicit val materializer = p.mat
  }

  case class FixtureParam(settings: MqttSourceSettings, server: Server, sys: ActorSystem, mat: Materializer)

}
