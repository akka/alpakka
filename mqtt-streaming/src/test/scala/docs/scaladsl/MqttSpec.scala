/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package docs.scaladsl
import akka.actor.ActorSystem
import akka.pattern.ask
import akka.stream.alpakka.mqtt.streaming._
import akka.stream.alpakka.mqtt.streaming.scaladsl.{ActorMqttClientSession, Mqtt}
import akka.stream.scaladsl.{Flow, Keep, Sink, Source}
import akka.stream.{ActorMaterializer, Materializer, OverflowStrategy}
import akka.testkit._
import akka.util.{ByteString, Timeout}
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}
import org.scalatest.concurrent.ScalaFutures

import scala.concurrent.duration._

class MqttSpec
    extends TestKit(ActorSystem("mqtt-spec"))
    with WordSpecLike
    with BeforeAndAfterAll
    with ScalaFutures
    with Matchers {

  implicit val mat: Materializer = ActorMaterializer()
  implicit val timeout: Timeout = Timeout(3.seconds.dilated)
  val timeoutDuration: FiniteDuration = timeout.duration

  val settings = MqttSessionSettings(100, timeoutDuration, timeoutDuration, timeoutDuration)

  import MqttCodec._

  "MQTT connector" should {

    "flow through a client session" in {
      val session = ActorMqttClientSession(settings)

      val server = TestProbe()
      val pipeToServer = Flow[ByteString].mapAsync(1)(msg => server.ref.ask(msg).mapTo[ByteString])

      val (client, result) =
        Source
          .queue(1, OverflowStrategy.fail)
          .via(
            Mqtt
              .clientSessionFlow(session)
              .join(pipeToServer)
          )
          .toMat(Sink.collection)(Keep.both)
          .run

      val connect = Connect("some-client-id", ConnectFlags.None)
      val connectBytes = connect.encode(ByteString.newBuilder).result()
      val connAck = ConnAck(ConnAckFlags.None, ConnAckReturnCode.ConnectionAccepted)
      val connAckBytes = connAck.encode(ByteString.newBuilder).result()

      val subscribe = Subscribe("some-topic")
      val subscribeBytes = subscribe.encode(ByteString.newBuilder, PacketId(1)).result()
      val subAck = SubAck(PacketId(1), List(ControlPacketFlags.QoSAtLeastOnceDelivery))
      val subAckBytes = subAck.encode(ByteString.newBuilder).result()

      val publish = Publish("some-topic", ByteString("some-payload"))
      val publishBytes = publish.encode(ByteString.newBuilder, Some(PacketId(0))).result()

      client.offer(Command(connect))

      server.expectMsg(connectBytes)
      server.reply(connAckBytes)

      client.offer(Command(subscribe))

      server.expectMsg(subscribeBytes)
      server.reply(subAckBytes)

      client.offer(Command(publish))

      server.expectMsg(publishBytes)
      server.reply(publishBytes) // FIXME: Should be pubAckBytes - once handled by the session

      client.complete()

      result.futureValue shouldBe List(Right(Event(connAck)), Right(Event(subAck)), Right(Event(publish)))
    }

    "Connect and carry through an object to ConnAck" in {
      val session = ActorMqttClientSession(settings)

      val server = TestProbe()
      val pipeToServer = Flow[ByteString].mapAsync(1)(msg => server.ref.ask(msg).mapTo[ByteString])

      val connect = Connect("some-client-id", ConnectFlags.None)
      val carry = "some-carry"

      val result =
        Source
          .single(Command(connect, carry))
          .via(
            Mqtt
              .clientSessionFlow(session)
              .join(pipeToServer)
          )
          .runWith(Sink.head)

      val connectBytes = connect.encode(ByteString.newBuilder).result()
      val connAck = ConnAck(ConnAckFlags.None, ConnAckReturnCode.ConnectionAccepted)
      val connAckBytes = connAck.encode(ByteString.newBuilder).result()

      server.expectMsg(connectBytes)
      server.reply(connAckBytes)

      result.futureValue shouldBe Right(Event(connAck, Some(carry)))
    }

    "disconnect when connected" in {
      val session = ActorMqttClientSession(settings)

      val server = TestProbe()
      val pipeToServer = Flow[ByteString].mapAsync(1)(msg => server.ref.ask(msg).mapTo[ByteString])

      val connect = Connect("some-client-id", ConnectFlags.None)
      val disconnect = Disconnect

      val client = Source
        .queue(1, OverflowStrategy.fail)
        .via(
          Mqtt
            .clientSessionFlow(session)
            .join(pipeToServer)
        )
        .toMat(Sink.ignore)(Keep.left)
        .run

      val connectBytes = connect.encode(ByteString.newBuilder).result()
      val connAck = ConnAck(ConnAckFlags.None, ConnAckReturnCode.ConnectionAccepted)
      val connAckBytes = connAck.encode(ByteString.newBuilder).result()

      val disconnectBytes = disconnect.encode(ByteString.newBuilder).result()

      client.offer(Command(connect))

      server.expectMsg(connectBytes)
      server.reply(connAckBytes)

      client.offer(Command(disconnect))

      server.expectMsg(disconnectBytes)
    }

    "subscribe to an existing topic" in {
      val session = ActorMqttClientSession(settings)

      val server = TestProbe()
      val pipeToServer = Flow[ByteString].mapAsync(1)(msg => server.ref.ask(msg).mapTo[ByteString])

      val connect = Connect("some-client-id", ConnectFlags.None)

      val client = Source
        .queue(1, OverflowStrategy.fail)
        .via(
          Mqtt
            .clientSessionFlow(session)
            .join(pipeToServer)
        )
        .toMat(Sink.ignore)(Keep.left)
        .run

      val connectBytes = connect.encode(ByteString.newBuilder).result()
      val connAck = ConnAck(ConnAckFlags.None, ConnAckReturnCode.ConnectionAccepted)
      val connAckBytes = connAck.encode(ByteString.newBuilder).result()

      val subscribe = Subscribe("some-topic")
      val subscribeBytes = subscribe.encode(ByteString.newBuilder, PacketId(1)).result()
      val subAck = SubAck(PacketId(1), List(ControlPacketFlags.QoSAtLeastOnceDelivery))
      val subAckBytes = subAck.encode(ByteString.newBuilder).result()

      client.offer(Command(connect))

      server.expectMsg(connectBytes)
      server.reply(connAckBytes)

      client.offer(Command(subscribe))

      server.expectMsg(subscribeBytes)
      server.reply(subAckBytes)

      client.offer(Command(subscribe))

      server.expectNoMessage(1.second.dilated)
    }
  }

  override def afterAll: Unit =
    TestKit.shutdownActorSystem(system)
}
