/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package docs.scaladsl
import akka.actor.ActorSystem
import akka.pattern.ask
import akka.stream.alpakka.mqtt.streaming._
import akka.stream.alpakka.mqtt.streaming.scaladsl.Mqtt
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

  val settings = SessionFlowSettings(100)

  "MQTT connector" should {

    "flow through a session" in {
      import MqttCodec._

      val server = TestProbe()

      val connect = Connect("some-client-id", ConnectFlags.None)
      val subscribe = Subscribe(PacketId(0), "some-topic")

      val result =
        Source(List(connect, subscribe))
          .via(
            Mqtt
              .sessionFlow(settings)
              .join(Flow[ByteString].mapAsync(1)(msg => server.ref.ask(msg).mapTo[ByteString]))
          )
          .runWith(Sink.collection)

      val connectBytes = connect.encode(ByteString.newBuilder).result()
      val connAck = ConnAck(ConnAckFlags.None, ConnAckReturnCode.ConnectionAccepted)
      val connAckBytes = connAck.encode(ByteString.newBuilder).result()

      val subscribeBytes = subscribe.encode(ByteString.newBuilder).result()
      val subAck = SubAck(PacketId(0), List(ControlPacketFlags.QoSAtMostOnceDelivery))
      val subAckBytes = subAck.encode(ByteString.newBuilder).result()

      val publishTopic = "some-topic"
      val publishPayload = ByteString("some-payload")
      val publish = Publish(publishTopic, publishPayload)
      val publishBytes = publish.encode(ByteString.newBuilder).result()

      server.expectMsg(connectBytes)
      server.reply(connAckBytes)

      server.expectMsg(subscribeBytes)
      server.reply(subAckBytes ++ publishBytes)

      result.futureValue shouldBe List(Right(connAck), Right(subAck), Right(publish))
    }
  }

  override def afterAll: Unit =
    TestKit.shutdownActorSystem(system)
}
