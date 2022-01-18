/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.scaladsl

import akka.Done
import akka.stream.alpakka.mqtt._
import akka.stream.alpakka.mqtt.scaladsl.{MqttFlow, MqttMessageWithAck}
import akka.stream.scaladsl.{Flow, Keep, Sink, Source}
import akka.util.ByteString

import scala.concurrent.{Future, Promise}

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

      subscribed.futureValue shouldBe Done
      mqttMessagePromise.success(None)
      noException should be thrownBy result.futureValue
    }

    "send an ack after sent confirmation" in {
      val topic = "flow-spec/topic-ack"

      //#create-flow-ack
      val mqttFlow: Flow[MqttMessageWithAck, MqttMessageWithAck, Future[Done]] =
        MqttFlow.atLeastOnceWithAck(
          connectionSettings,
          MqttSubscriptions(topic, MqttQoS.AtLeastOnce),
          bufferSize = 8,
          MqttQoS.AtLeastOnce
        )
      //#create-flow-ack

      val acked = Promise[Done]()

      class MqttMessageWithAckFake extends MqttMessageWithAck {
        override val message: MqttMessage = MqttMessage.create(topic, ByteString.fromString("ohi"))

        override def ack(): Future[Done] = {
          acked.trySuccess(Done)
          Future.successful(Done)
        }
      }

      val message = new MqttMessageWithAckFake

      val source = Source.single(message)

      //#run-flow-ack
      val (subscribed, result) = source
        .viaMat(mqttFlow)(Keep.right)
        .toMat(Sink.seq)(Keep.both)
        .run()

      //#run-flow-ack
      subscribed.futureValue shouldBe Done
      result.futureValue shouldBe empty

      acked.future.futureValue shouldBe Done
    }
  }
}
