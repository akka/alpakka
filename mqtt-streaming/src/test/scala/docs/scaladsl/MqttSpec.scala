/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package docs.scaladsl
import akka.actor.ActorSystem
import akka.pattern.ask
import akka.stream.alpakka.mqtt.streaming._
import akka.stream.alpakka.mqtt.streaming.scaladsl.{ActorMqttClientSession, Mqtt}
import akka.stream.scaladsl.{Flow, Sink, Source}
import akka.stream.{ActorMaterializer, Materializer}
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

  val settings = MqttSessionSettings(100, timeoutDuration, timeoutDuration)
  val session = ActorMqttClientSession(settings)

  "MQTT connector" should {

    "flow through a client session" in {
      import MqttCodec._

      val server = TestProbe()
      val pipeToServer = Flow[ByteString].mapAsync(1)(msg => server.ref.ask(msg).mapTo[ByteString])

      val connect = Connect("some-client-id", ConnectFlags.None)
      val subscribe = Subscribe("some-topic")

      val result =
        Source(List(Command(connect), Command(subscribe)))
          .via(
            Mqtt
              .clientSessionFlow(session)
              .join(pipeToServer)
          )
          .runWith(Sink.collection)

      val connectBytes = connect.encode(ByteString.newBuilder).result()
      val connAck = ConnAck(ConnAckFlags.None, ConnAckReturnCode.ConnectionAccepted)
      val connAckBytes = connAck.encode(ByteString.newBuilder).result()

      val subscribeBytes = subscribe.encode(ByteString.newBuilder, PacketId(0)).result()
      val subAck = SubAck(PacketId(0), List(ControlPacketFlags.QoSAtLeastOnceDelivery))
      val subAckBytes = subAck.encode(ByteString.newBuilder).result()

      val publish = Publish("some-topic", ByteString("some-payload"))
      val publishBytes = publish.encode(ByteString.newBuilder, Some(PacketId(0))).result()

      server.expectMsg(connectBytes)
      server.reply(connAckBytes)

      server.expectMsg(subscribeBytes)
      server.reply(subAckBytes ++ publishBytes)

      result.futureValue shouldBe List(Right(Event(connAck)), Right(Event(subAck)), Right(Event(publish)))
    }
  }

  override def afterAll: Unit =
    TestKit.shutdownActorSystem(system)
}
