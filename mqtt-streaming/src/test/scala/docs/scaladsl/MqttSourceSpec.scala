/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package docs.scaladsl

import java.time.LocalTime

import akka.Done
import akka.actor.ActorSystem
import akka.event.{Logging, LoggingAdapter}
import akka.stream._
import akka.stream.alpakka.mqtt.streaming._
import akka.stream.alpakka.mqtt.streaming.scaladsl._
import akka.stream.scaladsl.{Keep, Sink, Source, SourceQueueWithComplete, Tcp}
import akka.testkit.TestKit
import akka.stream.testkit.scaladsl.StreamTestKit.assertAllStagesStopped
import akka.util.ByteString
import org.scalatest._
import org.scalatest.concurrent.ScalaFutures

import scala.collection.immutable
import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration._

class MqttSourceSpec
    extends TestKit(ActorSystem("MqttSourceSpec"))
    with WordSpecLike
    with Matchers
    with BeforeAndAfterAll
    with ScalaFutures {

  private final val TopicPrefix = "streaming/source/topic/"

  private implicit val defaultPatience: PatienceConfig = PatienceConfig(10.seconds, interval = 500.millis)

  private implicit val mat: Materializer = ActorMaterializer()
  private implicit val ec: ExecutionContext = system.dispatcher
  private implicit val logging: LoggingAdapter = Logging.getLogger(system, this)

  val transportSettings = MqttTcpTransportSettings("localhost")

  override def afterAll(): Unit =
    TestKit.shutdownActorSystem(system)

  "At-most-once" should {
    "receive subscribed messages" in assertAllStagesStopped {
      val testId = "1"
      val clientId = s"streaming/source-spec/$testId"
      val topic = TopicPrefix + testId

      val sessionSettings = MqttSessionSettings()
      val time = LocalTime.now().toString
      val input = Vector("one-" + time, "two-" + time, "three-" + time, "four-" + time, "five-" + time)

      val mqttClientSession: MqttClientSession = ActorMqttClientSession(sessionSettings)

      val subscriptions = List(topic -> ControlPacketFlags.QoSAtLeastOnceDelivery)
      val (subscribed, received) = MqttSource
        .atMostOnce(mqttClientSession,
                    transportSettings,
                    MqttRestartSettings(),
                    MqttConnectionSettings(clientId),
                    subscriptions)
        .log("client received", p => p.payload.utf8String)
        .take(input.size)
        .toMat(Sink.seq)(Keep.both)
        .run()

      subscribed.futureValue should contain theSameElementsInOrderAs subscriptions

      val publishFlow = publish(topic, ControlPacketFlags.QoSAtMostOnceDelivery, input)

      received.futureValue.map(_.payload.utf8String) should contain theSameElementsAs input

      publishFlow.complete()
      publishFlow.watchCompletion().foreach { _ =>
        mqttClientSession.shutdown()
      }
    }

    "receive subscribed messages only once (AtMostOnceDelivery)" in assertAllStagesStopped {
      val testId = "2"
      val clientId = s"streaming/source-spec/$testId"
      val topic = TopicPrefix + testId

      val sessionSettings = MqttSessionSettings()
      val time = LocalTime.now().toString
      val input = Vector("one-" + time, "two-" + time, "three-" + time, "four-" + time, "five-" + time)

      val mqttClientSession: MqttClientSession = ActorMqttClientSession(sessionSettings)

      val subscriptions = List(topic -> ControlPacketFlags.QoSAtLeastOnceDelivery)
      val ((subscribed, switch), received) = MqttSource
        .atMostOnce(mqttClientSession,
                    transportSettings,
                    MqttRestartSettings(),
                    MqttConnectionSettings(clientId),
                    subscriptions)
        .log("client received", p => p.payload.utf8String)
        .viaMat(KillSwitches.single)(Keep.both)
        .toMat(Sink.seq)(Keep.both)
        .run()

      subscribed.futureValue should contain theSameElementsInOrderAs subscriptions

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
      val testId = "3"
      val clientId = s"streaming/source-spec/$testId"
      val topic = TopicPrefix + testId

      val sessionSettings = MqttSessionSettings()
      val time = LocalTime.now().toString
      val input = Vector("one-" + time, "two-" + time, "three-" + time, "four-" + time, "five-" + time)

      val mqttClientSession: MqttClientSession = ActorMqttClientSession(sessionSettings)

      val subscriptions = List(topic -> ControlPacketFlags.QoSAtLeastOnceDelivery)
      val ((subscribed, switch), received) = MqttSource
        .atMostOnce(mqttClientSession,
                    transportSettings,
                    MqttRestartSettings(),
                    MqttConnectionSettings(clientId),
                    subscriptions)
        .log("client received", p => p.payload.utf8String)
        .viaMat(KillSwitches.single)(Keep.both)
        .toMat(Sink.seq)(Keep.both)
        .run()

      subscribed.futureValue should contain theSameElementsInOrderAs subscriptions

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

  "At-least-once" should {
    "receive subscribed messages" in assertAllStagesStopped {
      val testId = "4"
      val clientId = s"streaming/source-spec/$testId"
      val topic = TopicPrefix + testId

      val sessionSettings = MqttSessionSettings()
      val time = LocalTime.now().toString
      val input = Vector("one-" + time, "two-" + time, "three-" + time, "four-" + time, "five-" + time)

      val mqttClientSession: MqttClientSession = ActorMqttClientSession(sessionSettings)

      var queue: immutable.Seq[Publish] = Vector[Publish]()

      val subscriptions = List(topic -> ControlPacketFlags.QoSAtLeastOnceDelivery)
      val (subscribed, switch) = MqttSource
        .atLeastOnce(mqttClientSession,
                     transportSettings,
                     MqttRestartSettings(),
                     MqttConnectionSettings(clientId),
                     subscriptions)
        .log("client received", p => p._1.payload.utf8String)
        .map {
          case in @ (publish, _) =>
            queue = queue ++ Vector(publish)
            in
        }
        .mapAsync(1) {
          case (_, ackHandle) =>
            ackHandle.ack()
        }
        .viaMat(KillSwitches.single)(Keep.both)
        .toMat(Sink.ignore)(Keep.left)
        .run()

      subscribed.futureValue should contain theSameElementsInOrderAs subscriptions
      val publishFlow = publish(topic, ControlPacketFlags.QoSAtLeastOnceDelivery, input)

      sleepToReceiveAll()
      switch.shutdown()

      queue.map(_.payload.utf8String) should contain theSameElementsAs input

      publishFlow.complete()
      publishFlow.watchCompletion().foreach { _ =>
        mqttClientSession.shutdown()
      }
    }

    "receive unacked messages later (when not using CleanSession)" in assertAllStagesStopped {
      val testId = "5"
      val time = LocalTime.now().toString
      val clientId = s"streaming/source-spec/$testId/$time"
      val topic = TopicPrefix + testId

      val sessionSettings = MqttSessionSettings()
      val input = Vector("one-" + time, "two-" + time, "three-" + time, "four-" + time, "five-" + time)
      val ackedInFirstBatch = 2

      val mqttClientSession: MqttClientSession = ActorMqttClientSession(sessionSettings)

      // read first elements
      val publishFlow: SourceQueueWithComplete[Command[Nothing]] = {
        val subscriptions = List(topic -> ControlPacketFlags.QoSAtLeastOnceDelivery)
        val (switch, received) = MqttSource
          .atLeastOnce(mqttClientSession,
                       transportSettings,
                       MqttRestartSettings(),
                       MqttConnectionSettings(clientId).withConnectFlags(ConnectFlags.None),
                       subscriptions)
          .zipWithIndex
          .mapAsync(1) {
            case ((publish, ackHandle), index) if index < ackedInFirstBatch =>
              ackHandle.ack().map(_ => publish)
            case ((publish, _), _) =>
              Future.successful(publish)
          }
          .viaMat(KillSwitches.single)(Keep.right)
          .toMat(Sink.seq)(Keep.both)
          .run()

        val publishFlow = publish(topic, ControlPacketFlags.QoSAtLeastOnceDelivery, input)
        sleepToReceiveAll()
        switch.shutdown()
        publishFlow.complete()
        publishFlow
      }
      // read elements that where not acked
      {
        val subscriptions = List(topic -> ControlPacketFlags.QoSAtLeastOnceDelivery)
        val (switch, received) = MqttSource
          .atLeastOnce(mqttClientSession,
                       transportSettings,
                       MqttRestartSettings(),
                       MqttConnectionSettings(clientId).withConnectFlags(ConnectFlags.None),
                       subscriptions)
          .mapAsync(1) {
            case (publish, ackHandle) =>
              ackHandle.ack().map(_ => publish)
          }
          .viaMat(KillSwitches.single)(Keep.right)
          .toMat(Sink.seq)(Keep.both)
          .run()

        sleepToReceiveAll()
        switch.shutdown()

        received.futureValue.map(_.payload.utf8String) should contain theSameElementsInOrderAs input.drop(
          ackedInFirstBatch
        )
      }

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
    val senderClientId = s"streaming/source-spec/sender"
    val sendSettings = MqttSessionSettings()
    val session = ActorMqttClientSession(sendSettings)
    val initialCommands = immutable.Seq(
      Command(Connect(senderClientId, ConnectFlags.CleanSession))
    )
    val commands =
      Source
        .queue[Command[Nothing]](10, OverflowStrategy.fail)
        .prepend(Source(initialCommands))
        .via(
          Mqtt
            .clientSessionFlow(session)
            .join(Tcp().outgoingConnection(transportSettings.host, transportSettings.port))
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
