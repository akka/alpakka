/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.mqtt.streaming

import akka.Done
import akka.actor.ActorSystem
import akka.stream.alpakka.mqtt.streaming.scaladsl.{ActorMqttClientSession, ActorMqttServerSession, Mqtt}
import akka.stream.scaladsl.{BroadcastHub, Keep, Sink, Source, Tcp}
import akka.stream.{ActorMaterializer, Materializer, OverflowStrategy}
import akka.util.ByteString
import org.openjdk.jmh.annotations._

import scala.concurrent.{Await, Promise}
import scala.concurrent.duration._

object MqttPerf {
  /*
   * An entry point for debugging purposes - invoke whatever you need to debug
   */
  def main(args: Array[String]): Unit = {
    val test = new MqttPerf()
    test.setup()
    try {
      test.serverPublish()
    } finally {
      test.tearDown()
    }
  }
}

@State(Scope.Benchmark)
class MqttPerf {

  import MqttCodec._

  private implicit val system: ActorSystem = ActorSystem("mqttperf")
  private implicit val mat: Materializer = ActorMaterializer()

  private val (client, clientSource) = Source
    .queue[Command[_]](2, OverflowStrategy.backpressure)
    .toMat(BroadcastHub.sink)(Keep.both)
    .run()

  private val (server, serverSource) = Source
    .queue[Command[_]](1, OverflowStrategy.backpressure)
    .toMat(BroadcastHub.sink)(Keep.both)
    .run()

  @Setup
  def setup(): Unit = {
    val host = "localhost"
    val port = 9883

    val connect = Connect("some-client-id", ConnectFlags.None)
    val connAck = ConnAck(ConnAckFlags.None, ConnAckReturnCode.ConnectionAccepted)
    val subscribe = Subscribe("some-topic")
    val subAck = SubAck(PacketId(1), List(ControlPacketFlags.QoSAtLeastOnceDelivery))
    val pubAck = PubAck(PacketId(1))

    val settings = MqttSessionSettings()
    val clientSession = ActorMqttClientSession(settings)
    val serverSession = ActorMqttServerSession(settings)

    val bound = Tcp()
      .bind(host, port)
      .flatMapMerge(
        1, { connection =>
          Source
            .fromGraph(serverSource)
            .via(
              Mqtt
                .serverSessionFlow(serverSession, ByteString(connection.remoteAddress.getAddress.getAddress))
                .join(connection.flow)
            )
            .wireTap(Sink.foreach[Either[DecodeError, Event[_]]] {
              case Right(Event(_: Connect, _)) =>
                server.offer(Command(connAck))
              case Right(Event(s: Subscribe, _)) =>
                server.offer(Command(subAck.copy(packetId = s.packetId)))
              case _ =>
            })
        }
      )
      .toMat(Sink.ignore)(Keep.left)
      .run()
    Await.ready(bound, 3.seconds)

    val subscribed = Promise[Done]

    Source
      .fromGraph(clientSource)
      .via(
        Mqtt
          .clientSessionFlow(clientSession)
          .join(Tcp().outgoingConnection(host, port))
      )
      .wireTap(Sink.foreach[Either[DecodeError, Event[_]]] {
        case Right(Event(s: SubAck, _)) =>
          subscribed.success(Done)
        case Right(Event(p: Publish, _)) =>
          client.offer(Command(pubAck.copy(packetId = p.packetId.get)))
        case _ =>
      })
      .runWith(Sink.ignore)

    client.offer(Command(connect))
    client.offer(Command(subscribe))
    Await.ready(subscribed.future, 3.seconds)
  }

  @Benchmark
  def serverPublish(): Unit =
    Await.ready(server.offer(Command(Publish("some-topic", ByteString("some-payload")))), 3.seconds)

  @TearDown
  def tearDown(): Unit =
    system.terminate()
}
