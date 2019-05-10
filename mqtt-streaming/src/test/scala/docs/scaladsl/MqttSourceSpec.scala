/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package docs.scaladsl

import java.time.LocalTime

import akka.{Done, NotUsed}
import akka.actor.ActorSystem
import akka.event.{Logging, LoggingAdapter}
import akka.stream._
// #imports
import akka.stream.alpakka.mqtt.streaming._
import akka.stream.alpakka.mqtt.streaming.scaladsl._
// #imports
import akka.stream.scaladsl.{Flow, Keep, Sink, Source, SourceQueueWithComplete}
import akka.stream.testkit.scaladsl.StreamTestKit.assertAllStagesStopped
import akka.testkit.TestKit
import akka.util.ByteString
import org.scalatest._
import org.scalatest.concurrent.ScalaFutures

import scala.collection.immutable
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}

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

  import MqttSourceSpec._

  "At-most-once" should {
    "receive subscribed messages" in assertAllStagesStopped {
      val testId = "1"
      val clientId = s"streaming/source-spec/$testId"
      val topic = TopicPrefix + testId

      val time = LocalTime.now().toString
      val input = Vector("one-" + time, "two-" + time, "three-" + time, "four-" + time, "five-" + time)

      val mqttClientSession: MqttClientSession = ActorMqttClientSession(MqttSessionSettings())

      val subscriptions = MqttSubscriptions(topic, ControlPacketFlags.QoSAtLeastOnceDelivery)
      val (subscribed, received) = MqttSource
        .atMostOnce(mqttClientSession,
                    connectionId = ByteString("MqttSourceSpec"),
                    transportSettings,
                    MqttRestartSettings(),
                    MqttConnectionSettings(clientId),
                    subscriptions)
        .take(input.size)
        .toMat(Sink.seq)(Keep.both)
        .run()

      subscribed.futureValue should contain theSameElementsInOrderAs subscriptions.subscriptions.toList

      val publishFlow = publish(topic, ControlPacketFlags.QoSAtMostOnceDelivery, transportSettings, input)

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

      val time = LocalTime.now().toString
      val input = Vector("one-" + time, "two-" + time, "three-" + time, "four-" + time, "five-" + time)

      val mqttClientSession: MqttClientSession = ActorMqttClientSession(MqttSessionSettings())

      val subscriptions = MqttSubscriptions.atLeastOnce(topic)
      val ((subscribed, switch), received) = MqttSource
        .atMostOnce(mqttClientSession,
                    connectionId = ByteString("MqttSourceSpec"),
                    transportSettings,
                    MqttRestartSettings(),
                    MqttConnectionSettings(clientId),
                    subscriptions)
        .viaMat(KillSwitches.single)(Keep.both)
        .toMat(Sink.seq)(Keep.both)
        .run()

      subscribed.futureValue should contain theSameElementsInOrderAs subscriptions.subscriptions.toList

      val publishFlow = publish(topic, ControlPacketFlags.QoSAtMostOnceDelivery, transportSettings, input)

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

      val subscriptions = MqttSubscriptions.atLeastOnce(topic)
      val ((subscribed, switch), received) = MqttSource
        .atMostOnce(mqttClientSession,
                    connectionId = ByteString("at-most-once-3"),
                    transportSettings,
                    MqttRestartSettings(),
                    MqttConnectionSettings(clientId),
                    subscriptions)
        .log("client received", p => p.payload.utf8String)
        .viaMat(KillSwitches.single)(Keep.both)
        .toMat(Sink.seq)(Keep.both)
        .run()

      subscribed.futureValue should contain theSameElementsInOrderAs subscriptions.subscriptions.toList

      val publishFlow = publish(topic, ControlPacketFlags.QoSAtLeastOnceDelivery, transportSettings, input)

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

      val time = LocalTime.now().toString
      val input = Vector("one-" + time, "two-" + time, "three-" + time, "four-" + time, "five-" + time)

      var queue: immutable.Seq[Publish] = Vector[Publish]()

      // #at-least-once

      val businessLogic: Flow[(Publish, MqttAckHandle), (Publish, MqttAckHandle), NotUsed] = // ???
        // #at-least-once
        Flow[(Publish, MqttAckHandle)]
          .map {
            case in @ (publish, _) =>
              queue = queue ++ Vector(publish)
              in
          }

      // #at-least-once

      val mqttClientSession: MqttClientSession = ActorMqttClientSession(MqttSessionSettings())
      val transportSettings = MqttTcpTransportSettings("localhost")
      val subscriptions = MqttSubscriptions.atLeastOnce(topic)

      val stream = MqttSource
        .atLeastOnce(mqttClientSession,
                     connectionId = ByteString("MqttSourceSpec"),
                     transportSettings,
                     MqttRestartSettings(),
                     MqttConnectionSettings(clientId),
                     subscriptions)
        .via(businessLogic)
        .mapAsync(1) {
          case (_, ackHandle) =>
            ackHandle.ack()
        }
        .viaMat(KillSwitches.single)(Keep.both)
        .toMat(Sink.ignore)(Keep.both)
        .run()

      val ((subscribed: Future[immutable.Seq[(String, ControlPacketFlags)]], switch: UniqueKillSwitch),
           streamCompletion: Future[Done]) = stream
      // #at-least-once

      subscribed.futureValue should contain theSameElementsInOrderAs subscriptions.subscriptions.toList
      val publishFlow = publish(topic, ControlPacketFlags.QoSAtLeastOnceDelivery, transportSettings, input)

      sleepToReceiveAll()
      // #at-least-once

      // stop the subscription
      switch.shutdown()
      // #at-least-once

      queue.map(_.payload.utf8String) should contain theSameElementsAs input

      publishFlow.complete()
      // #at-least-once

      streamCompletion.foreach { _ =>
        mqttClientSession.shutdown()
      }
      // #at-least-once
    }

    "receive unacked messages later (when not using CleanSession)" in assertAllStagesStopped {
      val testId = "5"
      val time = LocalTime.now().toString
      val connectionId = ByteString("MqttSourceSpec-2")
      val clientId = s"streaming/source-spec/$testId/$time"
      val topic = TopicPrefix + testId

      val sessionSettings = MqttSessionSettings()
      val input = Vector("one-" + time, "two-" + time, "three-" + time, "four-" + time, "five-" + time)
      val ackedInFirstBatch = 2

      val mqttClientSession: MqttClientSession = ActorMqttClientSession(sessionSettings)

      val connectionSettings = MqttConnectionSettings(clientId).withConnectFlags(ConnectFlags.None)
      val restartSettings = MqttRestartSettings()

      // read first elements
      val publishFlow: SourceQueueWithComplete[Command[Nothing]] = {
        val subscriptions = MqttSubscriptions.atLeastOnce(topic)
        val (switch, received) = MqttSource
          .atLeastOnce(
            mqttClientSession,
            connectionId,
            transportSettings,
            MqttRestartSettings(),
            connectionSettings,
            subscriptions
          )
          .log("client 1 received", p => p._1.payload.utf8String)
          .zipWithIndex
          .mapAsync(1) {
            case ((publish, ackHandle), index) if index < ackedInFirstBatch =>
              ackHandle.ack().map { _ =>
                logging.debug(s"acked ${publish.payload.utf8String}")
                publish
              }
            case ((publish, _), _) =>
              Future.successful(publish)
          }
          .viaMat(KillSwitches.single)(Keep.right)
          .toMat(Sink.seq)(Keep.both)
          .run()

        val publishFlow = publish(topic, ControlPacketFlags.QoSAtLeastOnceDelivery, transportSettings, input)
        sleepToReceiveAll()
        switch.shutdown()
        publishFlow.complete()
        publishFlow
      }
      //
      publishFlow.watchCompletion().futureValue shouldBe Done
      // read elements that where not acked
      val subscriptions = MqttSubscriptions.atLeastOnce(topic)
      val (switch, received) = MqttSource
        .atLeastOnce(
          mqttClientSession,
          connectionId,
          transportSettings,
          restartSettings,
          connectionSettings,
          subscriptions
        )
        .log("client 2 received", p => p._1.payload.utf8String)
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

}

object MqttSourceSpec {
  def publish(
      topic: String,
      delivery: ControlPacketFlags,
      transportSettings: MqttTransportSettings,
      input: immutable.Seq[String]
  )(implicit mat: Materializer,
    system: ActorSystem,
    ec: ExecutionContext): SourceQueueWithComplete[Command[Nothing]] = {
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
            .clientSessionFlow(session, ByteString("publisher"))
            .join(transportSettings.connectionFlow())
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
