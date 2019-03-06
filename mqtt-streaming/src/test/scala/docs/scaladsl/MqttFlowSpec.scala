/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package docs.scaladsl

import akka.{Done, NotUsed}
import akka.actor.ActorSystem
import akka.stream.alpakka.mqtt.streaming._
import akka.stream.alpakka.mqtt.streaming.scaladsl.{ActorMqttClientSession, ActorMqttServerSession, Mqtt}
import akka.stream.scaladsl.{BroadcastHub, Flow, Keep, Sink, Source, Tcp}
import akka.stream.testkit.scaladsl.StreamTestKit.assertAllStagesStopped
import akka.stream._
import akka.testkit.TestKit
import akka.util.ByteString
import org.scalatest._
import org.scalatest.concurrent.ScalaFutures

import scala.concurrent.Promise
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
    "establish a bidirectional connection and subscribe to a topic" in assertAllStagesStopped {
      val clientId = "source-spec/flow"
      val topic = "source-spec/topic1"

      //#create-streaming-flow
      val settings = MqttSessionSettings()
      val session = ActorMqttClientSession(settings)

      val connection = Tcp().outgoingConnection("localhost", 1883)

      val mqttFlow: Flow[Command[Nothing], Either[MqttCodec.DecodeError, Event[Nothing]], NotUsed] =
        Mqtt
          .clientSessionFlow(session)
          .join(connection)
      //#create-streaming-flow

      //#run-streaming-flow
      val (commands, events) =
        Source
          .queue(2, OverflowStrategy.fail)
          .via(mqttFlow)
          .collect {
            case Right(Event(p: Publish, _)) => p
          }
          .toMat(Sink.head)(Keep.both)
          .run()

      commands.offer(Command(Connect(clientId, ConnectFlags.CleanSession)))
      commands.offer(Command(Subscribe(topic)))
      session ! Command(
        Publish(ControlPacketFlags.RETAIN | ControlPacketFlags.QoSAtLeastOnceDelivery, topic, ByteString("ohi"))
      )
      //#run-streaming-flow

      events.futureValue match {
        case Publish(_, `topic`, _, bytes) => bytes shouldBe ByteString("ohi")
        case e => fail("Unexpected event: " + e)
      }

      //#run-streaming-flow

      // for shutting down properly
      commands.complete()
      session.shutdown()
      //#run-streaming-flow
    }
  }

  "mqtt server flow" should {
    "receive a bidirectional connection and a subscription to a topic" in assertAllStagesStopped {
      val clientId = "flow-spec/flow"
      val topic = "source-spec/topic1"
      val host = "localhost"
      val port = 9883

      //#create-streaming-bind-flow
      val settings = MqttSessionSettings()
      val session = ActorMqttServerSession(settings)

      val maxConnections = 1

      val bindSource =
        Tcp()
          .bind(host, port)
          .flatMapMerge(
            maxConnections, { connection =>
              val mqttFlow: Flow[Command[Nothing], Either[MqttCodec.DecodeError, Event[Nothing]], NotUsed] =
                Mqtt
                  .serverSessionFlow(session, ByteString(connection.remoteAddress.getAddress.getAddress))
                  .join(connection.flow)

              val (queue, source) = Source
                .queue[Command[Nothing]](3, OverflowStrategy.dropHead)
                .via(mqttFlow)
                .toMat(BroadcastHub.sink)(Keep.both)
                .run()

              val subscribed = Promise[Done]
              source
                .runForeach {
                  case Right(Event(_: Connect, _)) =>
                    queue.offer(Command(ConnAck(ConnAckFlags.None, ConnAckReturnCode.ConnectionAccepted)))
                  case Right(Event(cp: Subscribe, _)) =>
                    queue.offer(Command(SubAck(cp.packetId, cp.topicFilters.map(_._2)), Some(subscribed), None))
                  case Right(Event(publish @ Publish(flags, _, Some(packetId), _), _))
                      if flags.contains(ControlPacketFlags.RETAIN) =>
                    queue.offer(Command(PubAck(packetId)))
                    import mat.executionContext
                    subscribed.future.foreach(_ => session ! Command(publish))
                  case _ => // Ignore everything else
                }

              source
            }
          )
      //#create-streaming-bind-flow

      //#run-streaming-bind-flow
      val (bound, server) = bindSource
        .viaMat(KillSwitches.single)(Keep.both)
        .to(Sink.ignore)
        .run()
      //#run-streaming-bind-flow

      bound.futureValue.localAddress.getPort shouldBe port

      val clientSession = ActorMqttClientSession(settings)
      val connection = Tcp().outgoingConnection(host, port)
      val mqttFlow = Mqtt.clientSessionFlow(clientSession).join(connection)
      val (commands, events) =
        Source
          .queue(2, OverflowStrategy.fail)
          .via(mqttFlow)
          .collect {
            case Right(Event(p: Publish, _)) => p
          }
          .toMat(Sink.head)(Keep.both)
          .run()

      commands.offer(Command(Connect(clientId, ConnectFlags.None)))
      commands.offer(Command(Subscribe(topic)))
      clientSession ! Command(
        Publish(ControlPacketFlags.RETAIN | ControlPacketFlags.QoSAtLeastOnceDelivery, topic, ByteString("ohi"))
      )

      events.futureValue match {
        case Publish(_, `topic`, _, bytes) => bytes shouldBe ByteString("ohi")
        case e => fail("Unexpected event: " + e)
      }
      //#run-streaming-bind-flow

      // for shutting down properly
      server.shutdown()
      session.shutdown()
      //#run-streaming-bind-flow
    }
  }
}
