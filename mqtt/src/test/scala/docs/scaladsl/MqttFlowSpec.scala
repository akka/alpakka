/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package docs.scaladsl

import akka.Done
import akka.stream.alpakka.mqtt._
import akka.stream.alpakka.mqtt.scaladsl.{MqttFlow, MqttMessageWithAck}
import akka.stream.scaladsl.{Flow, Keep, Sink, Source}
import akka.util.ByteString
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence
import org.scalatest.time.{Seconds, Span}

import scala.concurrent.{Await, Future}

class MqttFlowSpec extends MqttSpecBase("MqttFlowSpec") {

  "mqtt flow" should {
    "establish a bidirectional connection and subscribe to a topic" in {
      val topic = "flow-spec/topic"
      //#create-flow
      val mqttFlow: Flow[MqttMessage, MqttMessage, Future[Done]] =
        MqttFlow.atMostOnce(
          connectionSettings.withClientId("flow-spec/flow"),
          MqttSubscriptions(topic, MqttQoS.AtLeastOnce),
          bufferSize = 8,
          MqttQoS.AtLeastOnce
        )
      //#create-flow

      val source = Source.maybe[MqttMessage]

      //#run-flow
      val ((mqttMessagePromise, subscribed), result) = source
        .viaMat(mqttFlow)(Keep.both)
        .toMat(Sink.seq)(Keep.both)
        .run()
      //#run-flow

      Await.ready(subscribed, timeout)
      mqttMessagePromise.success(None)
      noException should be thrownBy result.futureValue
    }
    "send an ack after sent confirmation" in {

      val topic = "flow-spec/topic-ack"
      val connectionSettings = MqttConnectionSettings(
        "tcp://localhost:1883",
        topic,
        new MemoryPersistence
      )

      //#create-flow-ack
      val mqttFlow: Flow[MqttMessageWithAck, MqttMessageWithAck, Future[Done]] =
        MqttFlow.atLeastOnceWithAck(
          connectionSettings,
          MqttSubscriptions(topic, MqttQoS.AtLeastOnce),
          bufferSize = 8,
          MqttQoS.AtLeastOnce
        )
      //#create-flow-ack

      class MqttMessageWithAckFake extends MqttMessageWithAck {
        var acked = false
        override val message: MqttMessage = MqttMessage.create(topic, ByteString.fromString("ohi"))

        override def ack(): Future[Done] = {
          acked = true
          println("[MqttMessageWithAck]")
          Future.successful(Done)
        }
      }

      val message = new MqttMessageWithAckFake
      message.acked shouldBe false

      val source = Source.single(message)

      //#run-flow-ack
      val ((_, subscribed), result) = source
        .viaMat(mqttFlow)(Keep.both)
        .toMat(Sink.seq)(Keep.both)
        .run()

      //#run-flow-ack
      Await.ready(subscribed, timeout)
      Await.ready(result, timeout)

      println("[Check]")
      eventually(timeout(Span(15, Seconds))) {
        Thread.sleep(10)
        message.acked should be(true)
      }
      message.acked shouldBe true
    }
  }
}
