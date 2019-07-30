/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package docs.scaladsl

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
import akka.stream.scaladsl.{BroadcastHub, Flow, Keep, Sink, Source, SourceQueueWithComplete}
import akka.stream.testkit.scaladsl.StreamTestKit.assertAllStagesStopped
import akka.stream.{ActorMaterializer, Materializer, OverflowStrategy}
import akka.testkit._
import akka.util.{ByteString, Timeout}
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.time.{Millis, Span}

import scala.concurrent.{ExecutionContext, Promise}
import scala.concurrent.duration._

class MqttSessionSpec
    extends TestKit(ActorSystem("mqtt-spec"))
    with WordSpecLike
    with BeforeAndAfterAll
    with ScalaFutures
    with Matchers {

  implicit val mat: Materializer = ActorMaterializer()
  implicit val executionContext: ExecutionContext = system.dispatcher
  implicit val timeout: Timeout = Timeout(3.seconds.dilated)

  val settings = MqttSessionSettings()

  import MqttCodec._

  "MQTT client connector" should {

    "flow through a client session" in assertAllStagesStopped {
      val session = ActorMqttClientSession(settings)

      val server = TestProbe()
      val pipeToServer = Flow[ByteString].mapAsync(1)(msg => server.ref.ask(msg).mapTo[ByteString])

      val (client, result) =
        Source
          .queue(1, OverflowStrategy.fail)
          .via(
            Mqtt
              .clientSessionFlow(session, ByteString("1"))
              .join(pipeToServer)
          )
          .take(3)
          .toMat(Sink.seq)(Keep.both)
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

      result.futureValue shouldBe List(Right(Event(connAck)), Right(Event(subAck)), Right(Event(pubAck)))
      client.complete()
      client.watchCompletion().foreach(_ => session.shutdown())
    }

    "Connect and carry through an object to ConnAck" in assertAllStagesStopped {
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
              .clientSessionFlow(session, ByteString("1"))
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
      client.complete()
      client.watchCompletion().foreach(_ => session.shutdown())
    }

    "Connect and stash any subsequent messages" in assertAllStagesStopped {
      val session = ActorMqttClientSession(settings)

      val server = TestProbe()
      val pipeToServer = Flow[ByteString].mapAsync(1)(msg => server.ref.ask(msg).mapTo[ByteString])

      val connect = Connect("some-client-id", ConnectFlags.None)
      val subscribe = Subscribe("some-topic")

      val (client, result) =
        Source
          .queue[Command[String]](2, OverflowStrategy.fail)
          .via(
            Mqtt
              .clientSessionFlow(session, ByteString("1"))
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
      client.complete()
      client.watchCompletion().foreach(_ => session.shutdown())
    }

    "Connect and fail given no ack" in assertAllStagesStopped {
      val session = ActorMqttClientSession(settings.withReceiveConnAckTimeout(0.seconds))

      val server = TestProbe()
      val pipeToServer = Flow[ByteString].mapAsync(1)(msg => server.ref.ask(msg).mapTo[ByteString])

      val connect = Connect("some-client-id", ConnectFlags.None)

      val (client, result) =
        Source
          .queue[Command[String]](1, OverflowStrategy.fail)
          .via(
            Mqtt
              .clientSessionFlow(session, ByteString("1"))
              .join(pipeToServer)
          )
          .toMat(Sink.ignore)(Keep.both)
          .run()

      client.offer(Command(connect))

      result.failed.futureValue shouldBe an[ActorMqttClientSession.ConnectFailed.type]
      client.complete()
      client.watchCompletion().foreach(_ => session.shutdown())
    }

    "Connect successfully, subscribe and fail given no ack" in assertAllStagesStopped {
      val session = ActorMqttClientSession(settings.withReceiveSubAckTimeout(0.seconds))

      val server = TestProbe()
      val pipeToServer = Flow[ByteString].mapAsync(1)(msg => server.ref.ask(msg).mapTo[ByteString])

      val connect = Connect("some-client-id", ConnectFlags.None)
      val subscribe = Subscribe("some-topic")

      val (client, result) =
        Source
          .queue[Command[String]](1, OverflowStrategy.fail)
          .via(
            Mqtt
              .clientSessionFlow(session, ByteString("1"))
              .join(pipeToServer)
          )
          .toMat(Sink.ignore)(Keep.both)
          .run()

      val connectBytes = connect.encode(ByteString.newBuilder).result()
      val connAck = ConnAck(ConnAckFlags.None, ConnAckReturnCode.ConnectionAccepted)
      val connAckBytes = connAck.encode(ByteString.newBuilder).result()

      client.offer(Command(connect))

      server.expectMsg(connectBytes)
      server.reply(connAckBytes)

      client.offer(Command(subscribe))

      result.failed.futureValue shouldBe an[ActorMqttClientSession.SubscribeFailed.type]
      client.complete()
      client.watchCompletion().foreach(_ => session.shutdown())
    }

    "disconnect when connected" in assertAllStagesStopped {
      val session = ActorMqttClientSession(settings)

      val server = TestProbe()
      val pipeToServer = Flow[ByteString].mapAsync(1)(msg => server.ref.ask(msg).mapTo[ByteString])

      val connect = Connect("some-client-id", ConnectFlags.None)
      val disconnect = Disconnect

      val (client, result) = Source
        .queue(1, OverflowStrategy.fail)
        .via(
          Mqtt
            .clientSessionFlow(session, ByteString("1"))
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
      client.complete()
      client.watchCompletion().foreach(_ => session.shutdown())
    }

    "disconnect when connection lost" in assertAllStagesStopped {
      val session = ActorMqttClientSession(settings)

      val server = TestProbe()
      val pipeToServer = Flow[ByteString].mapAsync(1)(msg => server.ref.ask(msg).mapTo[ByteString])

      val connect = Connect("some-client-id", ConnectFlags.None)

      val (client, result) = Source
        .queue(1, OverflowStrategy.fail)
        .via(
          Mqtt
            .clientSessionFlow(session, ByteString("1"))
            .join(pipeToServer)
        )
        .toMat(Sink.ignore)(Keep.both)
        .run()

      val connectBytes = connect.encode(ByteString.newBuilder).result()

      client.offer(Command(connect))

      server.expectMsg(connectBytes)

      client.complete()

      result.futureValue shouldBe Done
      client.watchCompletion().foreach(_ => session.shutdown())
    }

    "disconnect when connection lost while subscribing" in assertAllStagesStopped {
      val session = ActorMqttClientSession(settings)

      val server = TestProbe()
      val pipeToServer = Flow[ByteString].mapAsync(1)(msg => server.ref.ask(msg).mapTo[ByteString])

      val connect = Connect("some-client-id", ConnectFlags.None)
      val subscribe = Subscribe("some-topic")

      val (client, result) =
        Source
          .queue[Command[String]](2, OverflowStrategy.fail)
          .via(
            Mqtt
              .clientSessionFlow(session, ByteString("1"))
              .join(pipeToServer)
          )
          .toMat(Sink.ignore)(Keep.both)
          .run()

      val connectBytes = connect.encode(ByteString.newBuilder).result()
      val connAck = ConnAck(ConnAckFlags.None, ConnAckReturnCode.ConnectionAccepted)
      val connAckBytes = connAck.encode(ByteString.newBuilder).result()

      val subscribeBytes = subscribe.encode(ByteString.newBuilder, PacketId(1)).result()

      client.offer(Command(connect))
      client.offer(Command(subscribe))

      server.expectMsg(connectBytes)
      server.reply(connAckBytes)

      server.expectMsg(subscribeBytes)

      client.complete()

      result.futureValue shouldBe Done
      client.watchCompletion().foreach(_ => session.shutdown())
    }

    "correctly handle a new client connection" in assertAllStagesStopped {
      val session = ActorMqttClientSession(settings)

      val server = TestProbe()
      val pipeToServer = Flow[ByteString].mapAsync(1)(msg => server.ref.ask(msg).mapTo[ByteString])

      val connect = Connect("some-client-id", ConnectFlags.None)
      val carry = "some-carry"

      val connectBytes = connect.encode(ByteString.newBuilder).result()
      val connAck = ConnAck(ConnAckFlags.None, ConnAckReturnCode.ConnectionAccepted)
      val connAckBytes = connAck.encode(ByteString.newBuilder).result()

      val (firstClient, firstResult) =
        Source
          .queue[Command[String]](1, OverflowStrategy.fail)
          .via(
            Mqtt
              .clientSessionFlow(session, ByteString("1"))
              .join(pipeToServer)
          )
          .toMat(Sink.head)(Keep.both)
          .run()

      firstClient.offer(Command(connect, carry))

      server.expectMsg(connectBytes)
      server.reply(connAckBytes)

      firstResult.futureValue shouldBe Right(Event(connAck, Some(carry)))

      // we explicitly don't wait, as we want to test a race condition
      // where the new connection is established before the session
      // knows the first has finished/failed

      firstClient.complete()

      val (secondClient, secondResult) =
        Source
          .queue[Command[String]](1, OverflowStrategy.fail)
          .via(
            Mqtt
              .clientSessionFlow(session, ByteString("2"))
              .join(pipeToServer)
          )
          .toMat(Sink.head)(Keep.both)
          .run()

      secondClient.offer(Command(connect, carry))

      server.expectMsg(connectBytes)
      server.reply(connAckBytes)

      secondResult.futureValue shouldBe Right(Event(connAck, Some(carry)))

      secondClient.complete()

      for {
        _ <- firstClient.watchCompletion()
        _ <- secondClient.watchCompletion()
      } yield {
        session.shutdown()
      }
    }

    "receive a QoS 0 publication from a subscribed topic" in assertAllStagesStopped {
      val session = ActorMqttClientSession(settings)

      val server = TestProbe()
      val pipeToServer = Flow[ByteString].mapAsync(1)(msg => server.ref.ask(msg).mapTo[ByteString])

      val connect = Connect("some-client-id", ConnectFlags.None)

      val (client, result) = Source
        .queue(1, OverflowStrategy.fail)
        .via(
          Mqtt
            .clientSessionFlow(session, ByteString("1"))
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
      client.complete()
      client.watchCompletion().foreach(_ => session.shutdown())
    }

    "receive a QoS 1 publication from a subscribed topic and ack it" in assertAllStagesStopped {
      val session = ActorMqttClientSession(settings)

      val server = TestProbe()
      val pipeToServer = Flow[ByteString].mapAsync(1)(msg => server.ref.ask(msg).mapTo[ByteString])

      val connect = Connect("some-client-id", ConnectFlags.None)

      val publishReceived = Promise[Done]

      val client = Source
        .queue(1, OverflowStrategy.fail)
        .via(
          Mqtt
            .clientSessionFlow(session, ByteString("1"))
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
      client.complete()
      client.watchCompletion().foreach(_ => session.shutdown())
    }

    "receive a QoS 1 publication from a subscribed topic and ack it and then ack it again - the stream should ignore" in assertAllStagesStopped {
      // longer patience needed since Akka 2.6
      implicit val patienceConfig = PatienceConfig(scaled(1.second), scaled(50.millis))

      val session = ActorMqttClientSession(settings)

      val server = TestProbe()
      val pipeToServer = Flow[ByteString].mapAsync(1)(msg => server.ref.ask(msg).mapTo[ByteString])

      val connect = Connect("some-client-id", ConnectFlags.None)

      val publishReceived = Promise[Done]

      val (client, result) = Source
        .queue(1, OverflowStrategy.fail)
        .via(
          Mqtt
            .clientSessionFlow(session, ByteString("1"))
            .join(pipeToServer)
        )
        .collect {
          case Right(Event(cp: Publish, None)) => cp
        }
        .wireTap(_ => publishReceived.success(Done))
        .toMat(Sink.ignore)(Keep.both)
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

      val deliverPubAck1 = Promise[Done]
      client.offer(Command(pubAck, Some(deliverPubAck1), None))

      deliverPubAck1.future.futureValue shouldBe Done
      server.expectMsg(pubAckBytes)

      val deliverPubAck2 = Promise[Done]
      client.offer(Command(pubAck, Some(deliverPubAck2), None))

      deliverPubAck2.future.failed.futureValue shouldBe an[Exception]
      server.expectNoMessage()

      client.complete()
      result.futureValue shouldBe Done
      client.watchCompletion().foreach(_ => session.shutdown())
    }

    "receive a QoS 1 publication with DUP indicated from a unsubscribed topic - simulates a reconnect" in assertAllStagesStopped {
      val session = ActorMqttClientSession(settings)

      val server = TestProbe()
      val pipeToServer = Flow[ByteString].mapAsync(1)(msg => server.ref.ask(msg).mapTo[ByteString])

      val connect = Connect("some-client-id", ConnectFlags.None)

      val publishReceived = Promise[Done]

      val client = Source
        .queue(1, OverflowStrategy.fail)
        .via(
          Mqtt
            .clientSessionFlow(session, ByteString("1"))
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

      val publish = Publish(ControlPacketFlags.QoSAtLeastOnceDelivery | ControlPacketFlags.DUP,
                            "some-topic",
                            ByteString("some-payload"))
      val publishBytes = publish.encode(ByteString.newBuilder, Some(PacketId(1))).result()

      client.offer(Command(connect))

      server.expectMsg(connectBytes)
      server.reply(connAckBytes ++ publishBytes)

      publishReceived.future.futureValue shouldBe Done
      client.complete()
      client.watchCompletion().foreach(_ => session.shutdown())
    }

    "receive a QoS 2 publication from a subscribed topic and rec and comp it" in assertAllStagesStopped {
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
            .clientSessionFlow(session, ByteString("1"))
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
      client.complete()
      client.watchCompletion().foreach(_ => session.shutdown())
    }

    "publish with a QoS of 0" in assertAllStagesStopped {
      val session = ActorMqttClientSession(settings)

      val server = TestProbe()
      val pipeToServer = Flow[ByteString].mapAsync(1)(msg => server.ref.ask(msg).mapTo[ByteString])

      val client =
        Source
          .queue(1, OverflowStrategy.fail)
          .via(
            Mqtt
              .clientSessionFlow(session, ByteString("1"))
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
      client.complete()
      client.watchCompletion().foreach(_ => session.shutdown())
    }

    "publish and carry through an object to pubAck" in assertAllStagesStopped {
      val session = ActorMqttClientSession(settings)

      val server = TestProbe()
      val pipeToServer = Flow[ByteString].mapAsync(1)(msg => server.ref.ask(msg).mapTo[ByteString])

      val (client, result) =
        Source
          .queue(1, OverflowStrategy.fail)
          .via(
            Mqtt
              .clientSessionFlow[String](session, ByteString("1"))
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
      client.complete()
      client.watchCompletion().foreach(_ => session.shutdown())
    }

    "publish twice with a QoS of 1 so that the second is queued" in assertAllStagesStopped {
      val session = ActorMqttClientSession(settings)

      val server = TestProbe()
      val pipeToServer = Flow[ByteString].mapAsync(1)(msg => server.ref.ask(msg).mapTo[ByteString])

      val client =
        Source
          .queue(1, OverflowStrategy.fail)
          .via(
            Mqtt
              .clientSessionFlow(session, ByteString("1"))
              .join(pipeToServer)
          )
          .toMat(Sink.ignore)(Keep.left)
          .run()

      val connect = Connect("some-client-id", ConnectFlags.None)
      val connectBytes = connect.encode(ByteString.newBuilder).result()
      val connAck = ConnAck(ConnAckFlags.None, ConnAckReturnCode.ConnectionAccepted)
      val connAckBytes = connAck.encode(ByteString.newBuilder).result()

      val publish = Publish("some-topic", ByteString("some-payload"))
      val firstPublishBytes = publish.encode(ByteString.newBuilder, Some(PacketId(1))).result()
      val secondPublishBytes = publish.encode(ByteString.newBuilder, Some(PacketId(2))).result()
      val firstPubAck = PubAck(PacketId(1))
      val firstPubAckBytes = firstPubAck.encode(ByteString.newBuilder).result()
      val secondPubAck = PubAck(PacketId(2))
      val secondPubAckBytes = secondPubAck.encode(ByteString.newBuilder).result()

      client.offer(Command(connect))

      server.expectMsg(connectBytes)
      server.reply(connAckBytes)

      session ! Command(publish)
      session ! Command(publish)

      server.expectMsg(firstPublishBytes)
      server.reply(firstPubAckBytes)

      server.expectMsg(secondPublishBytes)
      server.reply(secondPubAckBytes)

      client.complete()
      client.watchCompletion().foreach(_ => session.shutdown())
    }

    "publish with a QoS of 1 and cause a retry given a timeout" in /* failing when enabled assertAllStagesStopped */ {
      val session = ActorMqttClientSession(settings.withProducerPubAckRecTimeout(10.millis))

      val server = TestProbe()
      val pipeToServer = Flow[ByteString].mapAsync(1)(msg => server.ref.ask(msg).mapTo[ByteString])

      val client =
        Source
          .queue(1, OverflowStrategy.fail)
          .via(
            Mqtt
              .clientSessionFlow(session, ByteString("1"))
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
      // This reply triggers an error showing in the logs which hinders proper shutdown
      //   7   │ 2019-03-06 11:28:23,035 ERROR [mqtt-spec-akka.actor.default-dispatcher-3] [akka.actor.OneForOneStrategy]  56 (of class java.lang.Integer)
      //   8   │ scala.MatchError: 56 (of class java.lang.Integer)
      //   9   │     at akka.stream.impl.fusing.GraphInterpreter.$anonfun$toSnapshot$4(GraphInterpreter.scala:662)
      server.reply(connAckBytes) // It doesn't matter what the message is - our test machinery here just wants a reply
      server.expectMsg(publishDupBytes)
      server.reply(pubAckBytes)

      client.complete()
      client.watchCompletion().foreach(_ => session.shutdown())
    }

    "publish with a QoS of 1 and cause a retry given a reconnect" in {
      val session = ActorMqttClientSession(settings.withProducerPubAckRecTimeout(0.millis))

      val server = TestProbe()
      val pipeToServer = Flow[ByteString].mapAsync(1)(msg => server.ref.ask(msg).mapTo[ByteString])

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

      val firstClient =
        Source
          .queue(1, OverflowStrategy.fail)
          .via(
            Mqtt
              .clientSessionFlow(session, ByteString("1"))
              .join(pipeToServer)
          )
          .toMat(Sink.ignore)(Keep.left)
          .run()

      firstClient.offer(Command(connect))

      server.expectMsg(connectBytes)
      server.reply(connAckBytes)

      session ! Command(publish)

      server.expectMsg(publishBytes)

      server.reply(connAckBytes)

      firstClient.complete()

      val secondClient =
        Source
          .queue(1, OverflowStrategy.fail)
          .via(
            Mqtt
              .clientSessionFlow(session, ByteString("2"))
              .join(pipeToServer)
          )
          .toMat(Sink.ignore)(Keep.left)
          .run()

      secondClient.offer(Command(connect))

      server.expectMsg(connectBytes)
      server.reply(connAckBytes)

      server.expectMsg(publishDupBytes)
      server.reply(pubAckBytes)

      secondClient.complete()

      for {
        _ <- firstClient.watchCompletion()
        _ <- secondClient.watchCompletion()
      } yield session.shutdown()
    }

    "publish with QoS 2 and carry through an object to pubComp" in assertAllStagesStopped {
      val session = ActorMqttClientSession(settings)

      val server = TestProbe()
      val pipeToServer = Flow[ByteString].mapAsync(1)(msg => server.ref.ask(msg).mapTo[ByteString])

      val (client, result) =
        Source
          .queue(1, OverflowStrategy.fail)
          .via(
            Mqtt
              .clientSessionFlow[String](session, ByteString("1"))
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

      client.complete()
      client.watchCompletion().foreach(_ => session.shutdown())
    }

    "connect and send out a ping request" in {
      /*assertAllStagesStopped { */
      val session = ActorMqttClientSession(settings)

      val server = TestProbe()
      val pipeToServer = Flow[ByteString].mapAsync(1)(msg => server.ref.ask(msg).mapTo[ByteString])

      val client =
        Source
          .queue(1, OverflowStrategy.fail)
          .via(
            Mqtt
              .clientSessionFlow(session, ByteString("1"))
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

      client.complete()
      client.watchCompletion().foreach(_ => session.shutdown())
    }

    "disconnect a connected session if a ping request is not replied to" in assertAllStagesStopped {
      val session = ActorMqttClientSession(settings)

      val server = TestProbe()
      val pipeToServer = Flow[ByteString].mapAsync(1)(msg => server.ref.ask(msg).mapTo[ByteString])

      val (client, result) =
        Source
          .queue(1, OverflowStrategy.fail)
          .via(
            Mqtt
              .clientSessionFlow(session, ByteString("1"))
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

      client.complete()
      client.watchCompletion().foreach(_ => session.shutdown())
    }

    "unsubscribe a client session" in assertAllStagesStopped {
      val session = ActorMqttClientSession(settings)

      val server = TestProbe()
      val pipeToServer = Flow[ByteString].mapAsync(1)(msg => server.ref.ask(msg).mapTo[ByteString])

      val unsubAck = UnsubAck(PacketId(1))
      val unsubAckReceived = Promise[Done]

      val (client, result) =
        Source
          .queue(2, OverflowStrategy.fail)
          .via(
            Mqtt
              .clientSessionFlow(session, ByteString("1"))
              .join(pipeToServer)
          )
          .drop(3)
          .wireTap { e =>
            e match {
              case Right(Event(`unsubAck`, None)) =>
                unsubAckReceived.success(Done)
              case _ =>
            }
          }
          .takeWhile {
            case Right(Event(PingResp, None)) => false
            case _ => true
          }
          .toMat(Sink.seq)(Keep.both)
          .run()

      val connect = Connect("some-client-id", ConnectFlags.None)
      val connectBytes = connect.encode(ByteString.newBuilder).result()
      val connAck = ConnAck(ConnAckFlags.None, ConnAckReturnCode.ConnectionAccepted)
      val connAckBytes = connAck.encode(ByteString.newBuilder).result()

      val subscribe = Subscribe("some-topic")
      val subscribeBytes = subscribe.encode(ByteString.newBuilder, PacketId(1)).result()
      val subAck = SubAck(PacketId(1), List(ControlPacketFlags.QoSAtLeastOnceDelivery))
      val subAckBytes = subAck.encode(ByteString.newBuilder).result()

      val unsubscribe = Unsubscribe("some-topic")
      val unsubscribeBytes = unsubscribe.encode(ByteString.newBuilder, PacketId(1)).result()
      val unsubAckBytes = unsubAck.encode(ByteString.newBuilder).result()

      val publish = Publish("some-topic", ByteString("some-payload"))
      val publishBytes = publish.encode(ByteString.newBuilder, Some(PacketId(1))).result()
      val publishDup = publish.copy(flags = publish.flags | ControlPacketFlags.DUP, packetId = Some(PacketId(1)))
      val publishDupBytes = publishDup.encode(ByteString.newBuilder, Some(PacketId(1))).result()

      val pingResp = PingResp
      val pingRespBytes = pingResp.encode(ByteString.newBuilder).result()
      val pubAck = PubAck(PacketId(1))
      val pubAckBytes = pubAck.encode(ByteString.newBuilder).result()

      client.offer(Command(connect))

      server.expectMsg(connectBytes)
      server.reply(connAckBytes)

      client.offer(Command(subscribe))

      server.expectMsg(subscribeBytes)
      server.reply(subAckBytes ++ publishBytes)

      client.offer(Command(unsubscribe))

      server.expectMsg(unsubscribeBytes)
      server.reply(unsubAckBytes ++ publishDupBytes)

      unsubAckReceived.future.futureValue shouldBe Done

      client.offer(Command(pubAck))

      server.expectMsg(pubAckBytes)
      server.reply(pingRespBytes)

      // Quite possible to receive a pub from an unsubscribed topic given that it may be in transit
      result.futureValue shouldBe Vector(Right(Event(unsubAck)), Right(Event(publishDup)))

      client.complete()
      client.watchCompletion().foreach(_ => session.shutdown())
    }

    "shutdown a session" in assertAllStagesStopped {
      val session = ActorMqttClientSession(settings)
      session.shutdown()

      val server = TestProbe()
      val pipeToServer = Flow[ByteString].mapAsync(1)(msg => server.ref.ask(msg).mapTo[ByteString])

      val (client, result) =
        Source
          .queue(1, OverflowStrategy.fail)
          .via(
            Mqtt
              .clientSessionFlow(session, ByteString("1"))
              .join(pipeToServer)
          )
          .toMat(Sink.headOption)(Keep.both)
          .run()

      val connect = Connect("some-client-id", ConnectFlags.None)

      client.offer(Command(connect))

      result.futureValue shouldBe None

      client.complete()
      client.watchCompletion().foreach(_ => session.shutdown())
    }

  }

  "MQTT server connector" should {

    "flow through a server session" in assertAllStagesStopped {
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
          .toMat(Sink.seq)(Keep.both)
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

      result.futureValue.head shouldBe Right(Event(connect))
      result.futureValue.apply(1) match {
        case Right(Event(s: Subscribe, _)) => s.topicFilters shouldBe subscribe.topicFilters
        case x => fail("Unexpected: " + x)
      }

      server.complete()
      server.watchCompletion().foreach(_ => session.shutdown())
    }

    "receive two subscriptions for the same topic" in assertAllStagesStopped {
      val session = ActorMqttServerSession(settings)

      val client = TestProbe()
      val toClient = Sink.foreach[ByteString](bytes => client.ref ! bytes)
      val (fromClientQueue, fromClient) = Source
        .queue[ByteString](2, OverflowStrategy.dropHead)
        .toMat(BroadcastHub.sink)(Keep.both)
        .run()

      val pipeToClient = Flow.fromSinkAndSource(toClient, fromClient)

      val connect = Connect("some-client-id", ConnectFlags.None)
      val connectReceived = Promise[Done]

      val subscribe = Subscribe("some-topic")
      val subscribe1Received = Promise[Done]
      val subscribe2Received = Promise[Done]

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
            case Right(Event(cp: Subscribe, _))
                if cp.topicFilters == subscribe.topicFilters && !subscribe1Received.isCompleted =>
              subscribe1Received.success(Done)
            case Right(Event(cp: Subscribe, _)) if cp.topicFilters == subscribe.topicFilters =>
              subscribe2Received.success(Done)
          })
          .toMat(Sink.ignore)(Keep.left)
          .run()

      val connectBytes = connect.encode(ByteString.newBuilder).result()
      val connAck = ConnAck(ConnAckFlags.None, ConnAckReturnCode.ConnectionAccepted)
      val connAckBytes = connAck.encode(ByteString.newBuilder).result()

      val subscribe1Bytes = subscribe.encode(ByteString.newBuilder, PacketId(1)).result()
      val sub1Ack = SubAck(PacketId(1), List(ControlPacketFlags.QoSAtLeastOnceDelivery))
      val sub1AckBytes = sub1Ack.encode(ByteString.newBuilder).result()

      val subscribe2Bytes = subscribe.encode(ByteString.newBuilder, PacketId(2)).result()
      val sub2Ack = SubAck(PacketId(2), List(ControlPacketFlags.QoSAtLeastOnceDelivery))
      val sub2AckBytes = sub2Ack.encode(ByteString.newBuilder).result()

      fromClientQueue.offer(connectBytes)

      connectReceived.future.futureValue shouldBe Done

      server.offer(Command(connAck))
      client.expectMsg(connAckBytes)

      fromClientQueue.offer(subscribe1Bytes)
      fromClientQueue.offer(subscribe2Bytes)

      subscribe1Received.future.futureValue shouldBe Done

      server.offer(Command(sub1Ack))
      client.expectMsg(sub1AckBytes)

      subscribe2Received.future.futureValue shouldBe Done

      server.offer(Command(sub2Ack))
      client.expectMsg(sub2AckBytes)

      fromClientQueue.complete()
      server.complete()
      server.watchCompletion().foreach(_ => session.shutdown())
    }

    "unsubscribe a server session" in assertAllStagesStopped {
      val session = ActorMqttServerSession(settings.withProducerPubAckRecTimeout(0.seconds))

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
            case Right(Event(cp: Subscribe, _)) if cp.topicFilters == subscribe.topicFilters =>
              subscribeReceived.success(Done)
            case Right(Event(cp: Unsubscribe, _)) if cp.topicFilters == unsubscribe.topicFilters =>
              unsubscribeReceived.success(Done)
          })
          .toMat(Sink.ignore)(Keep.left)
          .run()

      val connectBytes = connect.encode(ByteString.newBuilder).result()
      val connAck = ConnAck(ConnAckFlags.None, ConnAckReturnCode.ConnectionAccepted)
      val connAckBytes = connAck.encode(ByteString.newBuilder).result()

      val subscribeBytes = subscribe.encode(ByteString.newBuilder, PacketId(1)).result()
      val subAck = SubAck(PacketId(1), List(ControlPacketFlags.QoSAtLeastOnceDelivery))
      val subAckBytes = subAck.encode(ByteString.newBuilder).result()

      val publish = Publish("some-topic", ByteString("some-payload"))
      val publishBytes = publish.encode(ByteString.newBuilder, Some(PacketId(1))).result()

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

      session ! Command(publish)
      client.expectMsg(publishBytes)

      fromClientQueue.offer(unsubscribeBytes)

      unsubscribeReceived.future.foreach(_ => server.offer(Command(unsubAck)))(mat.executionContext)

      client.fishForSpecificMessage(3.seconds.dilated) {
        case `unsubAckBytes` =>
      }

      fromClientQueue.complete()
      server.complete()
      server.watchCompletion().foreach(_ => session.shutdown())
    }

    "reply to a ping request" in assertAllStagesStopped {
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
          .toMat(Sink.seq)(Keep.both)
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

      fromClientQueue.complete()
      server.complete()
      server.watchCompletion().foreach(_ => session.shutdown())
    }

    "close when no ping request received" in assertAllStagesStopped {
      // A longer patience config implicit is provided since minimum client's keep alive time is 1 second, so default
      // 150 millis is not enough for the ping request timeout to be triggered and verify the stream fails as expected.
      implicit val patienceConfig = PatienceConfig(scaled(Span(3000, Millis)), scaled(Span(15, Millis)))

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

      fromClientQueue.complete()
      server.complete()
      server.watchCompletion().foreach(_ => session.shutdown())
    }

    "notify on disconnect" in assertAllStagesStopped {
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

      session.watchClientSessions.runWith(Sink.head).futureValue shouldBe MqttServerSession.ClientSessionTerminated(
        clientId
      )

      fromClientQueue.complete()
      server.complete()
      server.watchCompletion().foreach(_ => session.shutdown())
    }

    def reconnectTest(explicitDisconnect: Boolean): Unit = {
      val session = ActorMqttServerSession(settings)

      val client = TestProbe()

      val connect = Connect("some-client-id", ConnectFlags.None)
      val firstConnectReceived = Promise[Done]
      val secondConnectReceived = Promise[Done]

      val subscribe = Subscribe("some-topic")
      val subscribeReceived = Promise[Done]

      val disconnect = Disconnect
      val disconnectReceived = Promise[Done]

      val publish = Publish("some-topic", ByteString("some-payload"))
      val publishReceived = Promise[Done]

      def server(
          connectionId: ByteString
      ): (SourceQueueWithComplete[ByteString], SourceQueueWithComplete[Command[Nothing]]) = {
        val toClient = Sink.foreach[ByteString](bytes => client.ref ! bytes)
        val (fromClientQueue, fromClient) = Source
          .queue[ByteString](1, OverflowStrategy.dropHead)
          .toMat(BroadcastHub.sink)(Keep.both)
          .run()

        val pipeToClient = Flow.fromSinkAndSource(toClient, fromClient)

        val connection = Source
          .queue[Command[Nothing]](1, OverflowStrategy.fail)
          .via(
            Mqtt
              .serverSessionFlow(session, connectionId)
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
        fromClientQueue -> connection
      }

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

      val (fromClientQueue1, serverConnection1) = server(ByteString(0))

      fromClientQueue1.offer(connectBytes)

      firstConnectReceived.future.futureValue shouldBe Done

      serverConnection1.offer(Command(connAck))
      client.expectMsg(connAckBytes)

      fromClientQueue1.offer(subscribeBytes)

      subscribeReceived.future.futureValue shouldBe Done

      serverConnection1.offer(Command(subAck))
      client.expectMsg(subAckBytes)

      if (explicitDisconnect) {
        fromClientQueue1.offer(disconnectBytes)

        disconnectReceived.future.futureValue shouldBe Done
      } else {
        serverConnection1.complete()
      }

      val (fromClientQueue2, serverConnection2) = server(ByteString(1))

      fromClientQueue2.offer(connectBytes)

      secondConnectReceived.future.futureValue shouldBe Done

      serverConnection2.offer(Command(connAck))
      client.expectMsg(connAckBytes)

      fromClientQueue2.offer(publishBytes)

      publishReceived.future.futureValue shouldBe Done

      serverConnection2.offer(Command(pubAck))
      client.expectMsg(pubAckBytes)

      session ! Command(publish)
      client.expectMsg(publishBytes)

      fromClientQueue1.complete()
      fromClientQueue2.complete()
      serverConnection1.complete()
      serverConnection2.complete()

      for {
        _ <- fromClientQueue1.watchCompletion()
        _ <- fromClientQueue2.watchCompletion()
        _ <- serverConnection1.watchCompletion()
        _ <- serverConnection2.watchCompletion()
      } session.shutdown()
    }

    "re-connect given connect, subscribe, disconnect, connect, publish" in assertAllStagesStopped {
      reconnectTest(explicitDisconnect = true)
    }

    "re-connect given connect, subscribe, connect again, publish" in assertAllStagesStopped {
      reconnectTest(explicitDisconnect = false)
    }

    "consume a duplicate publish on the server" in assertAllStagesStopped {
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
          .toMat(Sink.seq)(Keep.left)
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

      fromClientQueue.complete()
      server.complete()
      server.watchCompletion().foreach(_ => session.shutdown())
    }

    "produce a duplicate publish on the server given two client connections" in assertAllStagesStopped {
      val serverSession = ActorMqttServerSession(settings.withProducerPubAckRecTimeout(10.millis))

      val client1 = TestProbe()
      val toClient1 = Sink.foreach[ByteString](bytes => client1.ref ! bytes)
      val (client1Connection, fromClient1) = Source
        .queue[ByteString](1, OverflowStrategy.dropHead)
        .toMat(BroadcastHub.sink)(Keep.both)
        .run()

      val pipeToClient1 = Flow.fromSinkAndSource(toClient1, fromClient1)

      val client2 = TestProbe()
      val toClient2 = Sink.foreach[ByteString](bytes => client2.ref ! bytes)
      val (client2Connection, fromClient2) = Source
        .queue[ByteString](1, OverflowStrategy.dropHead)
        .toMat(BroadcastHub.sink)(Keep.both)
        .run()

      val pipeToClient2 = Flow.fromSinkAndSource(toClient2, fromClient2)

      val clientId = "some-client-id"

      val connect = Connect(clientId, ConnectFlags.None)
      val connect1Received = Promise[Done]
      val connect2Received = Promise[Done]

      val subscribe = Subscribe("some-topic")
      val subscribe1Received = Promise[Done]
      val subscribe2Received = Promise[Done]

      val pubAckReceived = Promise[Done]

      val disconnect = Disconnect
      val disconnectReceived = Promise[Done]

      val serverConnection1 =
        Source
          .queue[Command[Nothing]](1, OverflowStrategy.fail)
          .via(
            Mqtt
              .serverSessionFlow(serverSession, ByteString.empty)
              .join(pipeToClient1)
          )
          .wireTap(Sink.foreach[Either[DecodeError, Event[_]]] {
            case Right(Event(`connect`, _)) =>
              connect1Received.success(Done)
            case Right(Event(cp: Subscribe, _)) if cp.topicFilters == subscribe.topicFilters =>
              subscribe1Received.success(Done)
            case Right(Event(`disconnect`, _)) =>
              disconnectReceived.success(Done)
          })
          .toMat(Sink.seq)(Keep.left)
          .run()

      val connectBytes = connect.encode(ByteString.newBuilder).result()
      val connAck = ConnAck(ConnAckFlags.None, ConnAckReturnCode.ConnectionAccepted)
      val connAckBytes = connAck.encode(ByteString.newBuilder).result()

      val subscribeBytes = subscribe.encode(ByteString.newBuilder, PacketId(1)).result()
      val subAck = SubAck(PacketId(1), List(ControlPacketFlags.QoSAtLeastOnceDelivery))
      val subAckBytes = subAck.encode(ByteString.newBuilder).result()

      val publish = Publish("some-topic", ByteString("some-payload"))
      val publishBytes = publish.encode(ByteString.newBuilder, Some(PacketId(1))).result()
      val dupPublishBytes = publish
        .copy(flags = publish.flags | ControlPacketFlags.DUP)
        .encode(ByteString.newBuilder, Some(PacketId(1)))
        .result()
      val pubAck = PubAck(PacketId(1))
      val pubAckBytes = pubAck.encode(ByteString.newBuilder).result()

      val disconnectBytes = disconnect.encode(ByteString.newBuilder).result()

      client1Connection.offer(connectBytes)

      connect1Received.future.futureValue shouldBe Done

      serverConnection1.offer(Command(connAck))
      client1.expectMsg(connAckBytes)

      client1Connection.offer(subscribeBytes)

      subscribe1Received.future.futureValue shouldBe Done

      serverConnection1.offer(Command(subAck))
      client1.expectMsg(subAckBytes)

      serverSession ! Command(publish)
      client1.expectMsg(publishBytes)

      // Perform an explicit disconnect otherwise, if for example, we
      // just completed the client connection, the session may receive
      // the associated ConnectionLost signal for the new connection
      // given that the new connection occurs so quickly.
      client1Connection.offer(disconnectBytes)

      disconnectReceived.future.futureValue shouldBe Done

      val serverConnection2 =
        Source
          .queue[Command[Nothing]](1, OverflowStrategy.fail)
          .via(
            Mqtt
              .serverSessionFlow(serverSession, ByteString.empty)
              .join(pipeToClient2)
          )
          .wireTap(Sink.foreach[Either[DecodeError, Event[_]]] {
            case Right(Event(`connect`, _)) =>
              connect2Received.success(Done)
            case Right(Event(cp: Subscribe, _)) if cp.topicFilters == subscribe.topicFilters =>
              subscribe2Received.success(Done)
            case Right(Event(_: PubAck, _)) =>
              pubAckReceived.success(Done)
          })
          .toMat(Sink.seq)(Keep.left)
          .run()

      client2Connection.offer(connectBytes)

      connect2Received.future.futureValue shouldBe Done

      serverConnection2.offer(Command(connAck))
      client2.expectMsg(connAckBytes)

      client2Connection.offer(subscribeBytes)

      subscribe2Received.future.futureValue shouldBe Done

      serverConnection2.offer(Command(subAck))

      client2.fishForMessage(3.seconds.dilated) {
        case msg: ByteString if msg == dupPublishBytes => true
        case x => false
      }

      client2Connection.offer(pubAckBytes)
      pubAckReceived.future.futureValue shouldBe Done

      client1Connection.complete()
      client2Connection.complete()
      serverConnection1.complete()
      serverConnection2.complete()

      for {
        _ <- client1Connection.watchCompletion()
        _ <- client2Connection.watchCompletion()
        _ <- serverConnection1.watchCompletion()
        _ <- serverConnection2.watchCompletion()
      } serverSession.shutdown()

    }
  }

  override def afterAll: Unit =
    TestKit.shutdownActorSystem(system)
}
