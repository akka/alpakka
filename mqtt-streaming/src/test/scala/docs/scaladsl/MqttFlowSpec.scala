/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package docs.scaladsl

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.alpakka.mqtt.streaming._
import akka.stream.alpakka.mqtt.streaming.scaladsl.{ActorMqttClientSession, ActorMqttServerSession, Mqtt}
import akka.stream.scaladsl.{BroadcastHub, Flow, Keep, Sink, Source, Tcp}
import akka.stream._
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

  private implicit val defaultPatience: PatienceConfig = PatienceConfig(timeout = 5.seconds, interval = 100.millis)

  private implicit val mat: Materializer = ActorMaterializer()

  override def afterAll(): Unit =
    TestKit.shutdownActorSystem(system)

  "mqtt client flow" should {
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
        case e => fail("Unexpected event: " + e)
      }
    }
  }

  "mqtt server flow" should {
    "receive a bidirectional connection and a subscription to a topic" in {
      val clientId = "flow-spec/flow"
      val topic = "source-spec/topic1"

      //#create-streaming-bind-flow
      val settings = MqttSessionSettings()
      val session = ActorMqttServerSession(settings)

      val maxConnections = 1

      val bindSource =
        Tcp()
          .bind("localhost", 9883)
          .flatMapMerge(
            maxConnections, { connection =>
              val mqttFlow: Flow[Command[_], Either[MqttCodec.DecodeError, Event[_]], NotUsed] =
                Mqtt
                  .serverSessionFlow(session, ByteString(connection.remoteAddress.getAddress.getAddress))
                  .join(connection.flow)

              val (queue, source) = Source
                .queue[Command[_]](3, OverflowStrategy.dropHead)
                .via(mqttFlow)
                .toMat(BroadcastHub.sink)(Keep.both)
                .run()

              source
                .runForeach {
                  case Right(Event(_: Connect, _)) =>
                    queue.offer(Command(ConnAck(ConnAckFlags.None, ConnAckReturnCode.ConnectionAccepted)))
                  case Right(Event(cp: Subscribe, _)) =>
                    queue.offer(Command(SubAck(cp.packetId, cp.topicFilters.map(_._2))))
                  case Right(Event(publish @ Publish(_, _, Some(packetId), _), _)) =>
                    queue.offer(Command(PubAck(packetId)))
                    queue.offer(Command(publish))
                  case _ => // Ignore everything else
                }

              source
            }
          )
      //#create-streaming-bind-flow

      //#run-streaming-bind-flow
      bindSource.runWith(Sink.ignore)
      //#run-streaming-bind-flow

      val connection = Tcp().outgoingConnection("localhost", 9883)
      val mqttFlow = Mqtt.clientSessionFlow(ActorMqttClientSession(settings)).join(connection)
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

      events.futureValue match {
        case Right(Event(Publish(_, `topic`, _, bytes), _)) => bytes shouldBe ByteString("ohi")
        case e => fail("Unexpected event: " + e)
      }
    }
  }
}
