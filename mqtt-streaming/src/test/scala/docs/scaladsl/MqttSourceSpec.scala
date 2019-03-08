/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package docs.scaladsl

import akka.Done
import akka.actor.ActorSystem
import akka.event.{Logging, LoggingAdapter}
import akka.stream._
import akka.stream.alpakka.mqtt.streaming._
import akka.stream.alpakka.mqtt.streaming.scaladsl._
import akka.stream.scaladsl.{Keep, Sink, Source, Tcp}
import akka.testkit.TestKit
import akka.stream.testkit.scaladsl.StreamTestKit.assertAllStagesStopped
import akka.util.ByteString
import org.scalatest._
import org.scalatest.concurrent.ScalaFutures

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

class MqttSourceSpec
    extends TestKit(ActorSystem("MqttFlowSpec"))
    with WordSpecLike
    with Matchers
    with BeforeAndAfterAll
    with ScalaFutures {

  private implicit val defaultPatience: PatienceConfig = PatienceConfig(10.seconds, interval = 500.millis)

  private implicit val mat: Materializer = ActorMaterializer()
  private implicit val ec: ExecutionContext = system.dispatcher
  private implicit val logging: LoggingAdapter = Logging.getLogger(system, this)

  override def afterAll(): Unit =
    TestKit.shutdownActorSystem(system)

  "mqtt source" should {
    "receive subscribed messages" in assertAllStagesStopped {
      val clientId = "streaming/source-spec/flow1"
      val clientId2 = "streaming/source-spec/sender"
      val topic = "source-spec/topic1"

      val settings = MqttSessionSettings()
      val input = Vector("one", "two", "three", "four", "five")

      val mqttClientSession: MqttClientSession = ActorMqttClientSession(settings)

      val (subscribed, received) = MqttSource
        .atMostOnce(mqttClientSession, clientId, List(topic -> ControlPacketFlags.QoSAtLeastOnceDelivery))
        .log("received", p => p.payload.utf8String)
        .take(input.size)
        .toMat(Sink.seq)(Keep.both)
        .run()

      subscribed.futureValue shouldBe Done

      val session = ActorMqttClientSession(settings)
      val connection = Tcp().outgoingConnection("localhost", 1883)
      val commands =
        Source
          .queue(10, OverflowStrategy.fail)
          .via(
            Mqtt
              .clientSessionFlow(session)
              .join(connection)
          )
          .log("sender response")
          .to(Sink.ignore)
          .run()

      commands.offer(Command(Connect(clientId2, ConnectFlags.CleanSession)))
      for {
        data <- input
      } session ! Command(
        Publish(ControlPacketFlags.RETAIN, topic, ByteString(data))
      )

      received.futureValue.map(_.payload.utf8String) should contain theSameElementsAs input

      commands.complete()
      commands.watchCompletion().foreach { _ =>
        session.shutdown()
        mqttClientSession.shutdown()
      }
    }
  }

}
