/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package docs.scaladsl

import akka.{Done, NotUsed}
import akka.actor.ActorSystem
import akka.stream.alpakka.mqtt.streaming._
import akka.stream.alpakka.mqtt.streaming.scaladsl.{ActorMqttClientSession, Mqtt}
import akka.stream.scaladsl.{Flow, Keep, Sink, Source, Tcp}
import akka.stream.{ActorMaterializer, Materializer, OverflowStrategy}
import akka.testkit.TestKit
import akka.util.ByteString
import org.scalatest._
import org.scalatest.concurrent.ScalaFutures

import scala.concurrent.duration._

class MqttFlowSpec
    extends TestKit(ActorSystem("MqttFlowSpec"))
    with WordSpecLike
    with Matchers
    with BeforeAndAfterAll
    with ScalaFutures {

  private val timeout = 5.seconds
  private implicit val defaultPatience: PatienceConfig = PatienceConfig(timeout = 5.seconds, interval = 100.millis)

  private implicit val mat: Materializer = ActorMaterializer()

  override def afterAll(): Unit =
    TestKit.shutdownActorSystem(system)

  "mqtt flow" should {
    "establish a bidirectional connection and subscribe to a topic" in {
      val clientId = "flow-spec/flow"
      val topic = "source-spec/topic1"

      //#create-streaming-flow
      val settings = MqttSessionSettings()
      val session = ActorMqttClientSession(settings)

      val connection = Tcp().outgoingConnection("localhost", 1883)

      val mqttFlow: Flow[Command[_], Either[MqttCodec.DecodeError, Event[_]], NotUsed] =
        Mqtt
          .clientSessionFlow(session)
          .join(connection)
      //#create-streaming-flow

      //#run-streaming-flow
      val (commands, events) =
        Source
          .queue(3, OverflowStrategy.fail)
          .via(mqttFlow)
          .drop(3)
          .toMat(Sink.head)(Keep.both)
          .run()

      commands.offer(Command(Connect(clientId, ConnectFlags.None)))
      commands.offer(Command(Subscribe(topic)))
      commands.offer(Command(Publish(topic, ByteString("ohi"))))
      //#run-streaming-flow

      events.futureValue match {
        case Right(Event(Publish(_, `topic`, _, bytes), _)) => bytes shouldBe ByteString("ohi")
      }
    }
  }
}
