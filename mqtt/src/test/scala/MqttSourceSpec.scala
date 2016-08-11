/*
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.contrib.mqtt

import akka.actor.ActorSystem
import akka.stream._
import akka.stream.scaladsl.Keep
import akka.stream.testkit.scaladsl.TestSink
import akka.util.ByteString
import io.moquette.server.Server
import io.moquette.proto.messages.PublishMessage
import io.moquette.proto.messages.AbstractMessage.QOSType
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence
import org.scalatest._
import org.scalatest.concurrent.ScalaFutures
import scala.concurrent.duration._

class MqttSourceSpec extends WordSpec with Matchers with ScalaFutures {

  "mqtt source" should {
    "receive a message from a topic" in withBroker(Map("topic1" -> 0)) { settings => implicit server => implicit sys => implicit mat =>
      val (subscriptionFuture, probe) = MqttSource(settings, 8).toMat(TestSink.probe)(Keep.both).run()
      whenReady(subscriptionFuture) { _ =>
        publish("topic1", "ohi")
        probe.requestNext shouldBe MqttMessage("topic1", ByteString("ohi"))
      }
    }

    "receive messages from multiple topics" in withBroker(Map("topic1" -> 0, "topic2" -> 0)) { settings => implicit server => implicit sys => implicit mat =>
      val (subscriptionFuture, probe) = MqttSource(settings, 8).toMat(TestSink.probe)(Keep.both).run()
      whenReady(subscriptionFuture) { _ =>
        publish("topic1", "ohi")
        publish("topic2", "hello again")
        probe.requestNext shouldBe MqttMessage("topic1", ByteString("ohi"))
        probe.requestNext shouldBe MqttMessage("topic2", ByteString("hello again"))
      }
    }

    "fail stream when disconnected" in withBroker(Map("topic1" -> 0)) { settings => implicit server => implicit sys => implicit mat =>
      val (subscriptionFuture, probe) = MqttSource(settings, 8).toMat(TestSink.probe)(Keep.both).run()
      whenReady(subscriptionFuture) { _ =>
        publish("topic1", "ohi")
        probe.requestNext shouldBe MqttMessage("topic1", ByteString("ohi"))

        server.stopServer()
        probe.expectError.getMessage should be("Connection lost")
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

  def withBroker(subscriptions: Map[String, Int])(test: MqttSourceSettings => Server => ActorSystem => Materializer => Any) = {
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
      test(settings)(server)(sys)(mat)
    } finally {
      server.stopServer()
    }
  }

}
