/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.mqtt.streaming

import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.ReentrantLock

import akka.Done
import akka.actor.ActorSystem
import akka.stream.alpakka.mqtt.streaming
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
      for (_ <- 0 until 10000) test.serverPublish()
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
    .queue[Command[Nothing]](2, OverflowStrategy.backpressure)
    .toMat(BroadcastHub.sink)(Keep.both)
    .run()

  private val (server, serverSource) = Source
    .queue[Command[Nothing]](1, OverflowStrategy.backpressure)
    .toMat(BroadcastHub.sink)(Keep.both)
    .run()

  private val pubAckReceivedLock = new ReentrantLock()
  private val pubAckReceived = pubAckReceivedLock.newCondition()

  private val settings = MqttSessionSettings()
  private val clientSession = ActorMqttClientSession(settings)
  private val serverSession = ActorMqttServerSession(settings)

  @Setup
  def setup(): Unit = {
    val host = "localhost"
    val port = 9883

    val connect = Connect("some-client-id", ConnectFlags.None)
    val connAck = ConnAck(ConnAckFlags.None, ConnAckReturnCode.ConnectionAccepted)
    val subscribe = Subscribe("some-topic")
    val subAck = SubAck(PacketId(1), List(ControlPacketFlags.QoSAtLeastOnceDelivery))
    val pubAck = PubAck(PacketId(1))

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
              case Right(Event(_: PubAck, _)) =>
                pubAckReceivedLock.lock()
                try {
                  pubAckReceived.signal()
                } finally {
                  pubAckReceivedLock.unlock()
                }
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
          .clientSessionFlow(clientSession, ByteString("1"))
          .join(Tcp().outgoingConnection(host, port))
      )
      .wireTap(Sink.foreach[Either[DecodeError, Event[_]]] {
        case Right(Event(_: SubAck, _)) =>
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
  def serverPublish(): Unit = {
    serverSession ! streaming.Command(streaming.Publish("some-topic", ByteString("some-payload")))
    pubAckReceivedLock.lock()
    try {
      pubAckReceived.await(3, TimeUnit.SECONDS)
    } finally {
      pubAckReceivedLock.unlock()
    }
  }

  @TearDown
  def tearDown(): Unit =
    system.terminate()
}
