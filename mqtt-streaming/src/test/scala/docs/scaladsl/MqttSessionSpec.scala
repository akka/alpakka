/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package docs.scaladsl

import java.util.concurrent.atomic.AtomicInteger

import akka.Done
import akka.actor.ActorSystem
import akka.pattern.ask
import akka.stream.alpakka.mqtt.streaming._
import akka.stream.alpakka.mqtt.streaming.scaladsl.{
  ActorMqttClientSession,
  ActorMqttServerSession,
  Mqtt,
  MqttServerSession
}
import akka.stream.scaladsl.{BroadcastHub, Flow, Keep, Sink, Source}
import akka.stream.{ActorMaterializer, Materializer, OverflowStrategy, WatchedActorTerminatedException}
import akka.testkit._
import akka.util.{ByteString, Timeout}
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}
import org.scalatest.concurrent.ScalaFutures

import scala.concurrent.Promise
import scala.concurrent.duration._

class MqttSessionSpec
    extends TestKit(ActorSystem("mqtt-spec"))
    with WordSpecLike
    with BeforeAndAfterAll
    with ScalaFutures
    with Matchers {

  implicit val mat: Materializer = ActorMaterializer()
  implicit val timeout: Timeout = Timeout(3.seconds.dilated)

  val settings = MqttSessionSettings()

  import MqttCodec._

  "MQTT client connector" should {

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
          .run()

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

      session ! Command(publish)

      server.expectMsg(publishBytes)
      server.reply(pubAckBytes)

      client.complete()

      result.futureValue shouldBe List(Right(Event(connAck)), Right(Event(subAck)), Right(Event(pubAck)))
    }

    "Connect and carry through an object to ConnAck" in {
      val session = ActorMqttClientSession(settings)

      val server = TestProbe()
      val pipeToServer = Flow[ByteString].mapAsync(1)(msg => server.ref.ask(msg).mapTo[ByteString])

      val connect = Connect("some-client-id", ConnectFlags.None)
      val carry = "some-carry"

      val (client, result) =
        Source
          .queue[Command[String]](1, OverflowStrategy.fail)
          .via(
            Mqtt
              .clientSessionFlow(session)
              .join(pipeToServer)
          )
          .toMat(Sink.head)(Keep.both)
          .run()

      val connectBytes = connect.encode(ByteString.newBuilder).result()
      val connAck = ConnAck(ConnAckFlags.None, ConnAckReturnCode.ConnectionAccepted)
      val connAckBytes = connAck.encode(ByteString.newBuilder).result()

      client.offer(Command(connect, carry))

      server.expectMsg(connectBytes)
      server.reply(connAckBytes)

      result.futureValue shouldBe Right(Event(connAck, Some(carry)))
    }

    "Connect and stash any subsequent messages" in {
      val session = ActorMqttClientSession(settings)

      val server = TestProbe()
      val pipeToServer = Flow[ByteString].mapAsync(1)(msg => server.ref.ask(msg).mapTo[ByteString])

      val connect = Connect("some-client-id", ConnectFlags.None)
      val subscribe = Subscribe("some-topic")

      val (client, result) =
        Source
          .queue[Command[String]](1, OverflowStrategy.fail)
          .via(
            Mqtt
              .clientSessionFlow(session)
              .join(pipeToServer)
          )
          .drop(1)
          .toMat(Sink.head)(Keep.both)
          .run()

      val connectBytes = connect.encode(ByteString.newBuilder).result()
      val connAck = ConnAck(ConnAckFlags.None, ConnAckReturnCode.ConnectionAccepted)
      val connAckBytes = connAck.encode(ByteString.newBuilder).result()

      val subscribeBytes = subscribe.encode(ByteString.newBuilder, PacketId(1)).result()
      val subAck = SubAck(PacketId(1), List(ControlPacketFlags.QoSAtLeastOnceDelivery))
      val subAckBytes = subAck.encode(ByteString.newBuilder).result()

      client.offer(Command(connect))
      client.offer(Command(subscribe))

      server.expectMsg(connectBytes)
      server.reply(connAckBytes)

      server.expectMsg(subscribeBytes)
      server.reply(subAckBytes)

      result.futureValue shouldBe Right(Event(subAck))
    }

    "disconnect when connected" in {
      val session = ActorMqttClientSession(settings)

      val server = TestProbe()
      val pipeToServer = Flow[ByteString].mapAsync(1)(msg => server.ref.ask(msg).mapTo[ByteString])

      val connect = Connect("some-client-id", ConnectFlags.None)
      val disconnect = Disconnect

      val (client, result) = Source
        .queue(1, OverflowStrategy.fail)
        .via(
          Mqtt
            .clientSessionFlow(session)
            .join(pipeToServer)
        )
        .toMat(Sink.ignore)(Keep.both)
        .run()

      val connectBytes = connect.encode(ByteString.newBuilder).result()
      val connAck = ConnAck(ConnAckFlags.None, ConnAckReturnCode.ConnectionAccepted)
      val connAckBytes = connAck.encode(ByteString.newBuilder).result()

      val disconnectBytes = disconnect.encode(ByteString.newBuilder).result()

      client.offer(Command(connect))

      server.expectMsg(connectBytes)
      server.reply(connAckBytes)

      client.offer(Command(disconnect))

      server.expectMsg(disconnectBytes)

      result.futureValue shouldBe Done
    }

    "disconnect when connection lost" in {
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
        .toMat(Sink.ignore)(Keep.both)
        .run()

      val connectBytes = connect.encode(ByteString.newBuilder).result()

      client.offer(Command(connect))

      server.expectMsg(connectBytes)

      client.complete()

      result.futureValue shouldBe Done
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
        .run()

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
        .run()

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
        .run()

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
          .run()

      val connect = Connect("some-client-id", ConnectFlags.None)
      val connectBytes = connect.encode(ByteString.newBuilder).result()
      val connAck = ConnAck(ConnAckFlags.None, ConnAckReturnCode.ConnectionAccepted)
      val connAckBytes = connAck.encode(ByteString.newBuilder).result()

      val publish = Publish(ControlPacketFlags.QoSAtMostOnceDelivery, "some-topic", ByteString("some-payload"))
      val publishBytes = publish.encode(ByteString.newBuilder, None).result()

      client.offer(Command(connect))

      server.expectMsg(connectBytes)
      server.reply(connAckBytes)

      session ! Command(publish)

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
              .clientSessionFlow[String](session)
              .join(pipeToServer)
          )
          .drop(1)
          .toMat(Sink.head)(Keep.both)
          .run()

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

      session ! Command(publish, carry)

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
          .run()

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

      session ! Command(publish)
      session ! Command(publish)

      server.expectMsg(publishBytes)
      server.reply(pubAckBytes)

      server.expectMsg(publishBytes)
      server.reply(pubAckBytes)
    }

    "publish with a QoS of 1 and cause a retry given a timeout" in {
      val session = ActorMqttClientSession(settings.withReceivePubAckRecTimeout(10.millis))

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
          .run()

      val connect = Connect("some-client-id", ConnectFlags.None)
      val connectBytes = connect.encode(ByteString.newBuilder).result()
      val connAck = ConnAck(ConnAckFlags.None, ConnAckReturnCode.ConnectionAccepted)
      val connAckBytes = connAck.encode(ByteString.newBuilder).result()

      val publish = Publish("some-topic", ByteString("some-payload"))
      val publishBytes = publish.encode(ByteString.newBuilder, Some(PacketId(1))).result()
      val publishDup = publish.copy(flags = publish.flags | ControlPacketFlags.DUP)
      val publishDupBytes = publishDup.encode(ByteString.newBuilder, Some(PacketId(1))).result()
      val pubAck = PubAck(PacketId(1))
      val pubAckBytes = pubAck.encode(ByteString.newBuilder).result()

      client.offer(Command(connect))

      server.expectMsg(connectBytes)
      server.reply(connAckBytes)

      session ! Command(publish)

      server.expectMsg(publishBytes)
      server.reply(connAckBytes) // It doesn't matter what the message is - our test machinery here just wants a reply
      server.expectMsg(publishDupBytes)
      server.reply(pubAckBytes)
    }

    "publish with QoS 2 and carry through an object to pubComp" in {
      val session = ActorMqttClientSession(settings)

      val server = TestProbe()
      val pipeToServer = Flow[ByteString].mapAsync(1)(msg => server.ref.ask(msg).mapTo[ByteString])

      val (client, result) =
        Source
          .queue(1, OverflowStrategy.fail)
          .via(
            Mqtt
              .clientSessionFlow[String](session)
              .join(pipeToServer)
          )
          .drop(2)
          .toMat(Sink.head)(Keep.both)
          .run()

      val connect = Connect("some-client-id", ConnectFlags.None)
      val connectBytes = connect.encode(ByteString.newBuilder).result()
      val connAck = ConnAck(ConnAckFlags.None, ConnAckReturnCode.ConnectionAccepted)
      val connAckBytes = connAck.encode(ByteString.newBuilder).result()

      val publish = Publish(ControlPacketFlags.QoSExactlyOnceDelivery, "some-topic", ByteString("some-payload"))
      val publishBytes = publish.encode(ByteString.newBuilder, Some(PacketId(1))).result()
      val carry = "some-carry"
      val pubRec = PubRec(PacketId(1))
      val pubRecBytes = pubRec.encode(ByteString.newBuilder).result()
      val pubRel = PubRel(PacketId(1))
      val pubRelBytes = pubRel.encode(ByteString.newBuilder).result()
      val pubComp = PubComp(PacketId(1))
      val pubCompBytes = pubComp.encode(ByteString.newBuilder).result()

      client.offer(Command(connect))

      server.expectMsg(connectBytes)
      server.reply(connAckBytes)

      session ! Command(publish, carry)

      server.expectMsg(publishBytes)
      server.reply(pubRecBytes)

      server.expectMsg(pubRelBytes)
      server.reply(pubCompBytes)

      result.futureValue shouldBe Right(Event(pubComp, Some(carry)))
    }

    "connect and send out a ping request" in {
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
          .run()

      val connect = Connect("some-client-id", ConnectFlags.None).copy(keepAlive = 200.millis.dilated)
      val connectBytes = connect.encode(ByteString.newBuilder).result()
      val connAck = ConnAck(ConnAckFlags.None, ConnAckReturnCode.ConnectionAccepted)
      val connAckBytes = connAck.encode(ByteString.newBuilder).result()
      val pingReq = PingReq
      val pingReqBytes = pingReq.encode(ByteString.newBuilder).result()
      val pingResp = PingResp
      val pingRespBytes = pingResp.encode(ByteString.newBuilder).result()

      client.offer(Command(connect))

      server.expectMsg(connectBytes)
      server.reply(connAckBytes)

      server.expectMsg(pingReqBytes)
      server.reply(pingRespBytes)
      server.expectMsg(pingReqBytes)
      server.reply(pingRespBytes)
      server.expectMsg(pingReqBytes)
      server.reply(pingRespBytes)
    }

    "disconnect a connected session if a ping request is not replied to" in {
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
          .toMat(Sink.ignore)(Keep.both)
          .run()

      val connect = Connect("some-client-id", ConnectFlags.None).copy(keepAlive = 100.millis.dilated)
      val connectBytes = connect.encode(ByteString.newBuilder).result()
      val connAck = ConnAck(ConnAckFlags.None, ConnAckReturnCode.ConnectionAccepted)
      val connAckBytes = connAck.encode(ByteString.newBuilder).result()
      val pingReq = PingReq
      val pingReqBytes = pingReq.encode(ByteString.newBuilder).result()

      client.offer(Command(connect))

      server.expectMsg(connectBytes)
      server.reply(connAckBytes)

      server.expectMsg(pingReqBytes)

      result.failed.futureValue shouldBe ActorMqttClientSession.PingFailed
    }

    "unsubscribe a client session" in {
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
          .run()

      val connect = Connect("some-client-id", ConnectFlags.None)
      val connectBytes = connect.encode(ByteString.newBuilder).result()
      val connAck = ConnAck(ConnAckFlags.None, ConnAckReturnCode.ConnectionAccepted)
      val connAckBytes = connAck.encode(ByteString.newBuilder).result()

      val unsubscribe = Unsubscribe("some-topic")
      val unsubscribeBytes = unsubscribe.encode(ByteString.newBuilder, PacketId(1)).result()
      val unsubAck = UnsubAck(PacketId(1))
      val unsubAckBytes = unsubAck.encode(ByteString.newBuilder).result()

      client.offer(Command(connect))

      server.expectMsg(connectBytes)
      server.reply(connAckBytes)

      client.offer(Command(unsubscribe))

      server.expectMsg(unsubscribeBytes)
      server.reply(unsubAckBytes)

      result.futureValue shouldBe Right(Event(unsubAck))
    }

    "shutdown a session" in {
      val session = ActorMqttClientSession(settings)
      session.shutdown()

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
          .toMat(Sink.ignore)(Keep.both)
          .run()

      val connect = Connect("some-client-id", ConnectFlags.None)

      client.offer(Command(connect))

      result.failed.futureValue shouldBe a[WatchedActorTerminatedException]
    }

  }

  "MQTT server connector" should {

    "flow through a server session" in {
      val session = ActorMqttServerSession(settings)

      val client = TestProbe()
      val toClient = Sink.foreach[ByteString](bytes => client.ref ! bytes)
      val (fromClientQueue, fromClient) = Source
        .queue[ByteString](1, OverflowStrategy.dropHead)
        .toMat(BroadcastHub.sink)(Keep.both)
        .run()

      val pipeToClient = Flow.fromSinkAndSource(toClient, fromClient)

      val connect = Connect("some-client-id", ConnectFlags.None)
      val connectReceived = Promise[Done]

      val subscribe = Subscribe("some-topic")
      val subscribeReceived = Promise[Done]

      val unsubscribe = Unsubscribe("some-topic")
      val unsubscribeReceived = Promise[Done]

      val publish = Publish("some-topic", ByteString("some-payload"))
      val publishReceived = Promise[Done]

      val (server, result) =
        Source
          .queue[Command[Nothing]](1, OverflowStrategy.fail)
          .via(
            Mqtt
              .serverSessionFlow(session, ByteString.empty)
              .join(pipeToClient)
          )
          .wireTap(Sink.foreach[Either[DecodeError, Event[_]]] {
            case Right(Event(`connect`, _)) =>
              connectReceived.success(Done)
            case Right(Event(cp: Subscribe, _)) if cp.topicFilters == subscribe.topicFilters =>
              subscribeReceived.success(Done)
            case Right(Event(cp: Publish, _)) if cp.topicName == publish.topicName =>
              publishReceived.success(Done)
            case Right(Event(cp: Unsubscribe, _)) if cp.topicFilters == unsubscribe.topicFilters =>
              unsubscribeReceived.success(Done)
          })
          .toMat(Sink.collection)(Keep.both)
          .run()

      val connectBytes = connect.encode(ByteString.newBuilder).result()
      val connAck = ConnAck(ConnAckFlags.None, ConnAckReturnCode.ConnectionAccepted)
      val connAckBytes = connAck.encode(ByteString.newBuilder).result()

      val subscribeBytes = subscribe.encode(ByteString.newBuilder, PacketId(1)).result()
      val subAck = SubAck(PacketId(1), List(ControlPacketFlags.QoSAtLeastOnceDelivery))
      val subAckBytes = subAck.encode(ByteString.newBuilder).result()

      val publishBytes = publish.encode(ByteString.newBuilder, Some(PacketId(1))).result()
      val pubAck = PubAck(PacketId(1))
      val pubAckBytes = pubAck.encode(ByteString.newBuilder).result()

      val unsubscribeBytes = unsubscribe.encode(ByteString.newBuilder, PacketId(1)).result()
      val unsubAck = UnsubAck(PacketId(1))
      val unsubAckBytes = unsubAck.encode(ByteString.newBuilder).result()

      fromClientQueue.offer(connectBytes)

      connectReceived.future.futureValue shouldBe Done

      server.offer(Command(connAck))
      client.expectMsg(connAckBytes)

      fromClientQueue.offer(subscribeBytes)

      subscribeReceived.future.futureValue shouldBe Done

      server.offer(Command(subAck))
      client.expectMsg(subAckBytes)

      fromClientQueue.offer(publishBytes)

      publishReceived.future.futureValue shouldBe Done

      server.offer(Command(pubAck))
      client.expectMsg(pubAckBytes)

      session ! Command(publish)
      client.expectMsg(publishBytes)

      fromClientQueue.offer(unsubscribeBytes)

      unsubscribeReceived.future.futureValue shouldBe Done

      server.offer(Command(unsubAck))
      client.expectMsg(unsubAckBytes)

      fromClientQueue.complete()

      result.futureValue.apply(0) shouldBe Right(Event(connect))
      result.futureValue.apply(1) match {
        case Right(Event(s: Subscribe, _)) => s.topicFilters shouldBe subscribe.topicFilters
        case x => fail("Unexpected: " + x)
      }
    }

    "reply to a ping request" in {
      val session = ActorMqttServerSession(settings)

      val client = TestProbe()
      val toClient = Sink.foreach[ByteString](bytes => client.ref ! bytes)
      val (fromClientQueue, fromClient) = Source
        .queue[ByteString](1, OverflowStrategy.dropHead)
        .toMat(BroadcastHub.sink)(Keep.both)
        .run()

      val pipeToClient = Flow.fromSinkAndSource(toClient, fromClient)

      val connect = Connect("some-client-id", ConnectFlags.None)
      val connectReceived = Promise[Done]

      val pingReq = PingReq

      val (server, result) =
        Source
          .queue[Command[Nothing]](1, OverflowStrategy.fail)
          .via(
            Mqtt
              .serverSessionFlow(session, ByteString.empty)
              .join(pipeToClient)
          )
          .wireTap(Sink.foreach[Either[DecodeError, Event[_]]] {
            case Right(Event(`connect`, _)) => connectReceived.success(Done)
            case _ =>
          })
          .toMat(Sink.collection)(Keep.both)
          .run()

      val connectBytes = connect.encode(ByteString.newBuilder).result()
      val connAck = ConnAck(ConnAckFlags.None, ConnAckReturnCode.ConnectionAccepted)
      val connAckBytes = connAck.encode(ByteString.newBuilder).result()

      val pingReqBytes = pingReq.encode(ByteString.newBuilder).result()
      val pingResp = PingResp
      val pingRespBytes = pingResp.encode(ByteString.newBuilder).result()

      fromClientQueue.offer(connectBytes)

      connectReceived.future.futureValue shouldBe Done

      server.offer(Command(connAck))
      client.expectMsg(connAckBytes)

      fromClientQueue.offer(pingReqBytes)

      client.expectMsg(pingRespBytes)

      fromClientQueue.complete()

      result.futureValue shouldBe List(Right(Event(connect)), Right(Event(pingReq)))
    }

    "close when no ping request received" ignore { // https://github.com/akka/akka/issues/17997#issuecomment-429670321
      val session = ActorMqttServerSession(settings)

      val client = TestProbe()
      val toClient = Sink.foreach[ByteString](bytes => client.ref ! bytes)
      val (fromClientQueue, fromClient) = Source
        .queue[ByteString](1, OverflowStrategy.dropHead)
        .toMat(BroadcastHub.sink)(Keep.both)
        .run()

      val pipeToClient = Flow.fromSinkAndSource(toClient, fromClient)

      val connect = Connect("some-client-id", ConnectFlags.None).copy(keepAlive = 1.second.dilated)
      val connectReceived = Promise[Done]

      val (server, result) =
        Source
          .queue(1, OverflowStrategy.fail)
          .via(
            Mqtt
              .serverSessionFlow(session, ByteString.empty)
              .join(pipeToClient)
          )
          .wireTap(Sink.foreach[Either[DecodeError, Event[_]]] {
            case Right(Event(`connect`, _)) => connectReceived.success(Done)
            case _ =>
          })
          .toMat(Sink.ignore)(Keep.both)
          .run()

      val connectBytes = connect.encode(ByteString.newBuilder).result()
      val connAck = ConnAck(ConnAckFlags.None, ConnAckReturnCode.ConnectionAccepted)
      val connAckBytes = connAck.encode(ByteString.newBuilder).result()

      fromClientQueue.offer(connectBytes)

      connectReceived.future.futureValue shouldBe Done

      server.offer(Command(connAck))
      client.expectMsg(connAckBytes)

      result.failed.futureValue shouldBe ActorMqttServerSession.PingFailed
    }

    "notify on disconnect" in {
      val session = ActorMqttServerSession(settings.withReceiveConnectTimeout(100.millis.dilated))

      val client = TestProbe()
      val toClient = Sink.foreach[ByteString](bytes => client.ref ! bytes)
      val (fromClientQueue, fromClient) = Source
        .queue[ByteString](1, OverflowStrategy.dropHead)
        .toMat(BroadcastHub.sink)(Keep.both)
        .run()

      val pipeToClient = Flow.fromSinkAndSource(toClient, fromClient)

      val clientId = "some-client-id"

      val connect = Connect(clientId, ConnectFlags.None).copy(keepAlive = 1.second.dilated)
      val connectReceived = Promise[Done]

      val disconnect = Disconnect
      val disconnectReceived = Promise[Done]

      val server =
        Source
          .queue(1, OverflowStrategy.fail)
          .via(
            Mqtt
              .serverSessionFlow(session, ByteString.empty)
              .join(pipeToClient)
          )
          .wireTap(Sink.foreach[Either[DecodeError, Event[_]]] {
            case Right(Event(`connect`, _)) => connectReceived.success(Done)
            case Right(Event(`disconnect`, _)) => disconnectReceived.success(Done)
            case _ =>
          })
          .toMat(Sink.ignore)(Keep.left)
          .run()

      val connectBytes = connect.encode(ByteString.newBuilder).result()
      val connAck = ConnAck(ConnAckFlags.None, ConnAckReturnCode.ConnectionAccepted)
      val connAckBytes = connAck.encode(ByteString.newBuilder).result()

      val disconnectBytes = disconnect.encode(ByteString.newBuilder).result()

      fromClientQueue.offer(connectBytes)

      connectReceived.future.futureValue shouldBe Done

      server.offer(Command(connAck))
      client.expectMsg(connAckBytes)

      fromClientQueue.offer(disconnectBytes)

      disconnectReceived.future.futureValue shouldBe Done

      session.watchClientSessions.runWith(Sink.head).futureValue shouldBe
      MqttServerSession.ClientSessionTerminated(clientId)
    }

    def reconnectTest(explicitDisconnect: Boolean): Unit = {
      val session = ActorMqttServerSession(settings)

      val client = TestProbe()
      val toClient = Sink.foreach[ByteString](bytes => client.ref ! bytes)
      val (fromClientQueue, fromClient) = Source
        .queue[ByteString](1, OverflowStrategy.dropHead)
        .toMat(BroadcastHub.sink)(Keep.both)
        .run()

      val pipeToClient = Flow.fromSinkAndSource(toClient, fromClient)

      val connect = Connect("some-client-id", ConnectFlags.None)
      val firstConnectReceived = Promise[Done]
      val secondConnectReceived = Promise[Done]

      val subscribe = Subscribe("some-topic")
      val subscribeReceived = Promise[Done]

      val disconnect = Disconnect
      val disconnectReceived = Promise[Done]

      val publish = Publish("some-topic", ByteString("some-payload"))
      val publishReceived = Promise[Done]

      val connectionId = new AtomicInteger(0)

      val server =
        Source
          .queue[Command[Nothing]](1, OverflowStrategy.fail)
          .via(
            Mqtt
              .serverSessionFlow(session, ByteString(connectionId.getAndIncrement()))
              .join(pipeToClient)
          )
          .wireTap(Sink.foreach[Either[DecodeError, Event[_]]] {
            case Right(Event(`connect`, _)) if !firstConnectReceived.isCompleted =>
              firstConnectReceived.success(Done)
            case Right(Event(`connect`, _)) =>
              secondConnectReceived.success(Done)
            case Right(Event(cp: Subscribe, _)) if cp.topicFilters == subscribe.topicFilters =>
              subscribeReceived.success(Done)
            case Right(Event(`disconnect`, _)) =>
              disconnectReceived.success(Done)
            case Right(Event(cp: Publish, _)) if cp.topicName == publish.topicName =>
              publishReceived.success(Done)
          })
          .toMat(Sink.ignore)(Keep.left)
          .run()

      val connectBytes = connect.encode(ByteString.newBuilder).result()
      val connAck = ConnAck(ConnAckFlags.None, ConnAckReturnCode.ConnectionAccepted)
      val connAckBytes = connAck.encode(ByteString.newBuilder).result()

      val subscribeBytes = subscribe.encode(ByteString.newBuilder, PacketId(1)).result()
      val subAck = SubAck(PacketId(1), List(ControlPacketFlags.QoSAtLeastOnceDelivery))
      val subAckBytes = subAck.encode(ByteString.newBuilder).result()

      val disconnectBytes = disconnect.encode(ByteString.newBuilder).result()

      val publishBytes = publish.encode(ByteString.newBuilder, Some(PacketId(1))).result()
      val pubAck = PubAck(PacketId(1))
      val pubAckBytes = pubAck.encode(ByteString.newBuilder).result()

      fromClientQueue.offer(connectBytes)

      firstConnectReceived.future.futureValue shouldBe Done

      server.offer(Command(connAck))
      client.expectMsg(connAckBytes)

      fromClientQueue.offer(subscribeBytes)

      subscribeReceived.future.futureValue shouldBe Done

      server.offer(Command(subAck))
      client.expectMsg(subAckBytes)

      if (explicitDisconnect) {
        fromClientQueue.offer(disconnectBytes)

        disconnectReceived.future.futureValue shouldBe Done
      }

      fromClientQueue.offer(connectBytes)

      secondConnectReceived.future.futureValue shouldBe Done

      server.offer(Command(connAck))
      client.expectMsg(connAckBytes)

      fromClientQueue.offer(publishBytes)

      publishReceived.future.futureValue shouldBe Done

      server.offer(Command(pubAck))
      client.expectMsg(pubAckBytes)

      session ! Command(publish)
      client.expectMsg(publishBytes)
    }

    "re-connect given connect, subscribe, disconnect, connect, publish" in
    reconnectTest(explicitDisconnect = true)

    "re-connect given connect, subscribe, connect again, publish" in
    reconnectTest(explicitDisconnect = false)

    "receive a duplicate publish" in {
      val session = ActorMqttServerSession(settings)

      val client = TestProbe()
      val toClient = Sink.foreach[ByteString](bytes => client.ref ! bytes)
      val (fromClientQueue, fromClient) = Source
        .queue[ByteString](1, OverflowStrategy.dropHead)
        .toMat(BroadcastHub.sink)(Keep.both)
        .run()

      val pipeToClient = Flow.fromSinkAndSource(toClient, fromClient)

      val connect = Connect("some-client-id", ConnectFlags.None)
      val connectReceived = Promise[Done]

      val publish = Publish("some-topic", ByteString("some-payload"))
      val publishReceived = Promise[Done]
      val dupPublishReceived = Promise[Done]

      val server =
        Source
          .queue[Command[Nothing]](1, OverflowStrategy.fail)
          .via(
            Mqtt
              .serverSessionFlow(session, ByteString.empty)
              .join(pipeToClient)
          )
          .wireTap(Sink.foreach[Either[DecodeError, Event[_]]] {
            case Right(Event(`connect`, _)) =>
              connectReceived.success(Done)
            case Right(Event(cp: Publish, _)) if cp.flags.contains(ControlPacketFlags.DUP) =>
              dupPublishReceived.success(Done)
            case Right(Event(_: Publish, _)) =>
              publishReceived.success(Done)
          })
          .toMat(Sink.collection)(Keep.left)
          .run()

      val connectBytes = connect.encode(ByteString.newBuilder).result()
      val connAck = ConnAck(ConnAckFlags.None, ConnAckReturnCode.ConnectionAccepted)
      val connAckBytes = connAck.encode(ByteString.newBuilder).result()

      val publishBytes = publish.encode(ByteString.newBuilder, Some(PacketId(1))).result()
      val dupPublishBytes = publish
        .copy(flags = publish.flags | ControlPacketFlags.DUP)
        .encode(ByteString.newBuilder, Some(PacketId(1)))
        .result()
      val pubAck = PubAck(PacketId(1))
      val pubAckBytes = pubAck.encode(ByteString.newBuilder).result()

      fromClientQueue.offer(connectBytes)

      connectReceived.future.futureValue shouldBe Done

      server.offer(Command(connAck))
      client.expectMsg(connAckBytes)

      fromClientQueue.offer(publishBytes)

      publishReceived.future.futureValue shouldBe Done

      fromClientQueue.offer(dupPublishBytes)

      dupPublishReceived.future.futureValue shouldBe Done

      server.offer(Command(pubAck))
      client.expectMsg(pubAckBytes)
    }
  }

  override def afterAll: Unit =
    TestKit.shutdownActorSystem(system)
}
