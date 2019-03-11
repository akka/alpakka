/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package docs.scaladsl

import java.time.LocalTime
import java.util.UUID

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

import scala.collection.immutable
import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

class MqttSourceSpec
    extends TestKit(ActorSystem("MqttFlowSpec"))
    with WordSpecLike
    with Matchers
    with BeforeAndAfterAll
    with ScalaFutures {

  private final val TopicPrefix = "streaming/source/topic/"

  private implicit val defaultPatience: PatienceConfig = PatienceConfig(10.seconds, interval = 500.millis)

  private implicit val mat: Materializer = ActorMaterializer()
  private implicit val ec: ExecutionContext = system.dispatcher
  private implicit val logging: LoggingAdapter = Logging.getLogger(system, this)

  val mqttConnectionSettings = new MqttConnectionSettings()

  override def afterAll(): Unit =
    TestKit.shutdownActorSystem(system)

  "mqtt source" should {
    "receive subscribed messages" in assertAllStagesStopped {
      val clientId = s"streaming/source-spec/${UUID.randomUUID()}"
      val topic = TopicPrefix + "1"

      val sessionSettings = MqttSessionSettings()
      val time = LocalTime.now().toString
      val input = Vector("one-" + time, "two-" + time, "three-" + time, "four-" + time, "five-" + time)

      val mqttClientSession: MqttClientSession = ActorMqttClientSession(sessionSettings)

      val (subscribed, received) = MqttSource
        .atMostOnce(mqttClientSession,
                    mqttConnectionSettings,
                    new MqttRestartSettings(),
                    clientId,
                    List(topic -> ControlPacketFlags.QoSAtLeastOnceDelivery))
        .log("client received", p => p.payload.utf8String)
        .take(input.size)
        .toMat(Sink.seq)(Keep.both)
        .run()

      subscribed.futureValue shouldBe Done

      val publishFlow = publish(topic, ControlPacketFlags.QoSAtMostOnceDelivery, input)

      received.futureValue.map(_.payload.utf8String) should contain theSameElementsAs input

      publishFlow.complete()
      publishFlow.watchCompletion().foreach { _ =>
        mqttClientSession.shutdown()
      }
    }

    "receive subscribed messages only once (AtMostOnceDelivery)" in assertAllStagesStopped {
      val clientId = s"streaming/source-spec/12"
      val topic = TopicPrefix + "2"

      val sessionSettings = MqttSessionSettings()
      val time = LocalTime.now().toString
      val input = Vector("one-" + time, "two-" + time, "three-" + time, "four-" + time, "five-" + time)

      val mqttClientSession: MqttClientSession = ActorMqttClientSession(sessionSettings)

      val ((subscribed, switch), received) = MqttSource
        .atMostOnce(mqttClientSession,
                    mqttConnectionSettings,
                    new MqttRestartSettings(),
                    clientId,
                    List(topic -> ControlPacketFlags.QoSAtLeastOnceDelivery))
        .log("client received", p => p.payload.utf8String)
        .viaMat(KillSwitches.single)(Keep.both)
        .toMat(Sink.seq)(Keep.both)
        .run()

      subscribed.futureValue shouldBe Done

      val publishFlow = publish(topic, ControlPacketFlags.QoSAtMostOnceDelivery, input)

      sleepToReceiveAll()
      switch.shutdown()

      val elements = received.futureValue.map(_.payload.utf8String)
      elements should contain theSameElementsAs input
      elements should have(
        'size (input.size)
      )

      publishFlow.complete()
      publishFlow.watchCompletion().foreach { _ =>
        mqttClientSession.shutdown()
      }
    }

    "receive subscribed messages only once (AtLeastOnceDelivery)" in assertAllStagesStopped {
      val clientId = s"streaming/source-spec/14"
      val topic = TopicPrefix + "3"

      val sessionSettings = MqttSessionSettings()
      val time = LocalTime.now().toString
      val input = Vector("one-" + time, "two-" + time, "three-" + time, "four-" + time, "five-" + time)

      val mqttClientSession: MqttClientSession = ActorMqttClientSession(sessionSettings)

      val ((subscribed, switch), received) = MqttSource
        .atMostOnce(mqttClientSession,
                    mqttConnectionSettings,
                    new MqttRestartSettings(),
                    clientId,
                    List(topic -> ControlPacketFlags.QoSAtLeastOnceDelivery))
        .log("client received", p => p.payload.utf8String)
        .viaMat(KillSwitches.single)(Keep.both)
        .toMat(Sink.seq)(Keep.both)
        .run()

      subscribed.futureValue shouldBe Done

      val publishFlow = publish(topic, ControlPacketFlags.QoSAtLeastOnceDelivery, input)

      sleepToReceiveAll()
      switch.shutdown()

      val elements = received.futureValue.map(_.payload.utf8String)
      elements should contain theSameElementsAs input
      elements should have(
        'size (input.size)
      )

      publishFlow.complete()
      publishFlow.watchCompletion().foreach { _ =>
        mqttClientSession.shutdown()
      }
    }
  }

  private def sleepToReceiveAll(): Unit =
    sleep(2.seconds, "to make sure we don't get more than expected")

  private def sleep(d: FiniteDuration, msg: String): Unit = {
    logging.debug(s"sleeping $d $msg")
    Thread.sleep(d.toMillis)
  }

  private def publish(topic: String, delivery: ControlPacketFlags, input: Vector[String]) = {
    val clientId2 = s"streaming/source-spec/sender-${UUID.randomUUID()}"
    val sendSettings = MqttSessionSettings()
    val session = ActorMqttClientSession(sendSettings)
    val initialCommands = immutable.Seq(
      Command(Connect(clientId2, ConnectFlags.CleanSession))
    )
    val commands =
      Source
        .queue[Command[Nothing]](10, OverflowStrategy.fail)
        .prepend(Source(initialCommands))
        .via(
          Mqtt
            .clientSessionFlow(session)
            .join(Tcp().outgoingConnection(mqttConnectionSettings.host, mqttConnectionSettings.port))
        )
        .log("sender response")
        .to(Sink.ignore)
        .run()

    for {
      data <- input
    } {
      session ! Command(
        Publish(delivery, topic, ByteString(data))
      )
    }

    commands.watchCompletion().foreach { _ =>
      session.shutdown()
    }
    commands
  }
}
