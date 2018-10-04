/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package docs.scaladsl

import akka.Done
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

import scala.concurrent.Promise
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

  val settings = MqttSessionSettings(100,
                                     timeoutDuration,
                                     1,
                                     1,
                                     timeoutDuration,
                                     timeoutDuration,
                                     timeoutDuration,
                                     timeoutDuration,
                                     timeoutDuration)

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
          .take(3)
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
      val publishBytes = publish.encode(ByteString.newBuilder, Some(PacketId(1))).result()
      val pubAck = PubAck(PacketId(1))
      val pubAckBytes = pubAck.encode(ByteString.newBuilder).result()

      client.offer(Command(connect))

      server.expectMsg(connectBytes)
      server.reply(connAckBytes)

      client.offer(Command(subscribe))

      server.expectMsg(subscribeBytes)
      server.reply(subAckBytes)

      client.offer(Command(publish))

      server.expectMsg(publishBytes)
      server.reply(pubAckBytes)

      result.futureValue shouldBe List(Right(Event(connAck)), Right(Event(subAck)), Right(Event(pubAck)))
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

    "receive a QoS 0 publication from a subscribed topic" in {
      val session = ActorMqttClientSession(settings)

      val server = TestProbe()
      val pipeToServer = Flow[ByteString].mapAsync(1)(msg => server.ref.ask(msg).mapTo[ByteString])

      val connect = Connect("some-client-id", ConnectFlags.None)

      val (client, result) = Source
        .queue(1, OverflowStrategy.fail)
        .via(
          Mqtt
            .clientSessionFlow(session)
            .join(pipeToServer)
        )
        .drop(2)
        .toMat(Sink.head)(Keep.both)
        .run

      val connectBytes = connect.encode(ByteString.newBuilder).result()
      val connAck = ConnAck(ConnAckFlags.None, ConnAckReturnCode.ConnectionAccepted)
      val connAckBytes = connAck.encode(ByteString.newBuilder).result()

      val subscribe = Subscribe("some-topic")
      val subscribeBytes = subscribe.encode(ByteString.newBuilder, PacketId(1)).result()
      val subAck = SubAck(PacketId(1), List(ControlPacketFlags.QoSAtLeastOnceDelivery))
      val subAckBytes = subAck.encode(ByteString.newBuilder).result()

      val publish = Publish(ControlPacketFlags.QoSAtMostOnceDelivery, "some-topic", ByteString("some-payload"))
      val publishBytes = publish.encode(ByteString.newBuilder, None).result()

      client.offer(Command(connect))

      server.expectMsg(connectBytes)
      server.reply(connAckBytes)

      client.offer(Command(subscribe))

      server.expectMsg(subscribeBytes)
      server.reply(subAckBytes ++ publishBytes)

      result.futureValue shouldBe Right(Event(publish))
    }

    "receive a QoS 1 publication from a subscribed topic and ack it" in {
      val session = ActorMqttClientSession(settings)

      val server = TestProbe()
      val pipeToServer = Flow[ByteString].mapAsync(1)(msg => server.ref.ask(msg).mapTo[ByteString])

      val connect = Connect("some-client-id", ConnectFlags.None)

      val publishReceived = Promise[Done]

      val client = Source
        .queue(1, OverflowStrategy.fail)
        .via(
          Mqtt
            .clientSessionFlow(session)
            .join(pipeToServer)
        )
        .collect {
          case Right(Event(cp: Publish, None)) => cp
        }
        .wireTap(_ => publishReceived.success(Done))
        .toMat(Sink.ignore)(Keep.left)
        .run

      val connectBytes = connect.encode(ByteString.newBuilder).result()
      val connAck = ConnAck(ConnAckFlags.None, ConnAckReturnCode.ConnectionAccepted)
      val connAckBytes = connAck.encode(ByteString.newBuilder).result()

      val subscribe = Subscribe("some-topic")
      val subscribeBytes = subscribe.encode(ByteString.newBuilder, PacketId(1)).result()
      val subAck = SubAck(PacketId(1), List(ControlPacketFlags.QoSAtLeastOnceDelivery))
      val subAckBytes = subAck.encode(ByteString.newBuilder).result()

      val publish = Publish(ControlPacketFlags.QoSAtLeastOnceDelivery, "some-topic", ByteString("some-payload"))
      val publishBytes = publish.encode(ByteString.newBuilder, Some(PacketId(1))).result()
      val pubAck = PubAck(PacketId(1))
      val pubAckBytes = pubAck.encode(ByteString.newBuilder).result()

      client.offer(Command(connect))

      server.expectMsg(connectBytes)
      server.reply(connAckBytes)

      client.offer(Command(subscribe))

      server.expectMsg(subscribeBytes)
      server.reply(subAckBytes ++ publishBytes)

      publishReceived.future.futureValue shouldBe Done

      client.offer(Command(pubAck))

      server.expectMsg(pubAckBytes)
    }

    "receive a QoS 2 publication from a subscribed topic and rec and comp it" in {
      val session = ActorMqttClientSession(settings)

      val server = TestProbe()
      val pipeToServer = Flow[ByteString].mapAsync(1)(msg => server.ref.ask(msg).mapTo[ByteString])

      val connect = Connect("some-client-id", ConnectFlags.None)

      val publishReceived = Promise[Done]
      val pubRelReceived = Promise[Done]

      val client = Source
        .queue(1, OverflowStrategy.fail)
        .via(
          Mqtt
            .clientSessionFlow(session)
            .join(pipeToServer)
        )
        .collect {
          case Right(Event(cp: Publish, None)) => cp
          case Right(Event(cp: PubRel, None)) => cp
        }
        .wireTap { e =>
          e match {
            case _: Publish => publishReceived.success(Done)
            case _: PubRel => pubRelReceived.success(Done)
            case _ =>
          }
        }
        .toMat(Sink.ignore)(Keep.left)
        .run

      val connectBytes = connect.encode(ByteString.newBuilder).result()
      val connAck = ConnAck(ConnAckFlags.None, ConnAckReturnCode.ConnectionAccepted)
      val connAckBytes = connAck.encode(ByteString.newBuilder).result()

      val subscribe = Subscribe("some-topic")
      val subscribeBytes = subscribe.encode(ByteString.newBuilder, PacketId(1)).result()
      val subAck = SubAck(PacketId(1), List(ControlPacketFlags.QoSAtLeastOnceDelivery))
      val subAckBytes = subAck.encode(ByteString.newBuilder).result()

      val publish = Publish(ControlPacketFlags.QoSExactlyOnceDelivery, "some-topic", ByteString("some-payload"))
      val publishBytes = publish.encode(ByteString.newBuilder, Some(PacketId(1))).result()
      val pubRec = PubRec(PacketId(1))
      val pubRecBytes = pubRec.encode(ByteString.newBuilder).result()
      val pubRel = PubRel(PacketId(1))
      val pubRelBytes = pubRel.encode(ByteString.newBuilder).result()
      val pubComp = PubComp(PacketId(1))
      val pubCompBytes = pubComp.encode(ByteString.newBuilder).result()

      client.offer(Command(connect))

      server.expectMsg(connectBytes)
      server.reply(connAckBytes)

      client.offer(Command(subscribe))

      server.expectMsg(subscribeBytes)
      server.reply(subAckBytes ++ publishBytes)

      publishReceived.future.futureValue shouldBe Done

      client.offer(Command(pubRec))

      server.expectMsg(pubRecBytes)
      server.reply(pubRelBytes)

      pubRelReceived.future.futureValue shouldBe Done

      client.offer(Command(pubComp))

      server.expectMsg(pubCompBytes)
    }

    "publish with a QoS of 0" in {
      val session = ActorMqttClientSession(settings)

      val server = TestProbe()
      val pipeToServer = Flow[ByteString].mapAsync(1)(msg => server.ref.ask(msg).mapTo[ByteString])

      val client =
        Source
          .queue(1, OverflowStrategy.fail)
          .via(
            Mqtt
              .clientSessionFlow(session)
              .join(pipeToServer)
          )
          .toMat(Sink.ignore)(Keep.left)
          .run

      val connect = Connect("some-client-id", ConnectFlags.None)
      val connectBytes = connect.encode(ByteString.newBuilder).result()
      val connAck = ConnAck(ConnAckFlags.None, ConnAckReturnCode.ConnectionAccepted)
      val connAckBytes = connAck.encode(ByteString.newBuilder).result()

      val publish = Publish(ControlPacketFlags.QoSAtMostOnceDelivery, "some-topic", ByteString("some-payload"))
      val publishBytes = publish.encode(ByteString.newBuilder, None).result()

      client.offer(Command(connect))

      server.expectMsg(connectBytes)
      server.reply(connAckBytes)

      client.offer(Command(publish))

      server.expectMsg(publishBytes)
    }

    "publish and carry through an object to pubAck" in {
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
          .drop(1)
          .toMat(Sink.head)(Keep.both)
          .run

      val connect = Connect("some-client-id", ConnectFlags.None)
      val connectBytes = connect.encode(ByteString.newBuilder).result()
      val connAck = ConnAck(ConnAckFlags.None, ConnAckReturnCode.ConnectionAccepted)
      val connAckBytes = connAck.encode(ByteString.newBuilder).result()

      val publish = Publish("some-topic", ByteString("some-payload"))
      val publishBytes = publish.encode(ByteString.newBuilder, Some(PacketId(1))).result()
      val carry = "some-carry"
      val pubAck = PubAck(PacketId(1))
      val pubAckBytes = pubAck.encode(ByteString.newBuilder).result()

      client.offer(Command(connect))

      server.expectMsg(connectBytes)
      server.reply(connAckBytes)

      client.offer(Command(publish, carry))

      server.expectMsg(publishBytes)
      server.reply(pubAckBytes)

      result.futureValue shouldBe Right(Event(pubAck, Some(carry)))
    }

    "publish twice with a QoS of 1 so that the second is queued" in {
      val session = ActorMqttClientSession(settings)

      val server = TestProbe()
      val pipeToServer = Flow[ByteString].mapAsync(1)(msg => server.ref.ask(msg).mapTo[ByteString])

      val client =
        Source
          .queue(1, OverflowStrategy.fail)
          .via(
            Mqtt
              .clientSessionFlow(session)
              .join(pipeToServer)
          )
          .toMat(Sink.ignore)(Keep.left)
          .run

      val connect = Connect("some-client-id", ConnectFlags.None)
      val connectBytes = connect.encode(ByteString.newBuilder).result()
      val connAck = ConnAck(ConnAckFlags.None, ConnAckReturnCode.ConnectionAccepted)
      val connAckBytes = connAck.encode(ByteString.newBuilder).result()

      val publish = Publish("some-topic", ByteString("some-payload"))
      val publishBytes = publish.encode(ByteString.newBuilder, Some(PacketId(1))).result()
      val pubAck = PubAck(PacketId(1))
      val pubAckBytes = pubAck.encode(ByteString.newBuilder).result()

      client.offer(Command(connect))

      server.expectMsg(connectBytes)
      server.reply(connAckBytes)

      client.offer(Command(publish))
      client.offer(Command(publish))

      server.expectMsg(publishBytes)
      server.reply(pubAckBytes)

      server.expectMsg(publishBytes)
      server.reply(pubAckBytes)
    }
  }

  override def afterAll: Unit =
    TestKit.shutdownActorSystem(system)
}
