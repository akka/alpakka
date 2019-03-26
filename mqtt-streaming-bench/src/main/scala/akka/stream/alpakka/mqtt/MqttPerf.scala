/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.mqtt

import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.ReentrantLock

import akka.actor.ActorSystem
import akka.stream.alpakka.mqtt.scaladsl.MqttFlow
import akka.stream.alpakka.mqtt.streaming.scaladsl.{ActorMqttServerSession, Mqtt}
import akka.stream.scaladsl.{BroadcastHub, Keep, Sink, Source, Tcp}
import akka.stream.{ActorMaterializer, Materializer, OverflowStrategy}
import akka.util.ByteString
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence
import org.openjdk.jmh.annotations._

import scala.concurrent.duration._
import scala.concurrent.Await

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

  import streaming.MqttCodec._

  private implicit val system: ActorSystem = ActorSystem("mqttperf")
  private implicit val mat: Materializer = ActorMaterializer()

  private val (_, clientSource) = Source
    .queue[MqttMessage](2, OverflowStrategy.backpressure)
    .toMat(BroadcastHub.sink)(Keep.both)
    .run()

  private val (server, serverSource) = Source
    .queue[streaming.Command[Nothing]](1, OverflowStrategy.backpressure)
    .toMat(BroadcastHub.sink)(Keep.both)
    .run()

  private val pubAckReceivedLock = new ReentrantLock()
  private val pubAckReceived = pubAckReceivedLock.newCondition()

  private val serverSession = ActorMqttServerSession(streaming.MqttSessionSettings())

  @Setup
  def setup(): Unit = {
    val host = "localhost"
    val port = 9883

    val connectionSettings = MqttConnectionSettings(s"tcp://$host:$port", "some-client-id", new MemoryPersistence)

    val connAck = streaming.ConnAck(streaming.ConnAckFlags.None, streaming.ConnAckReturnCode.ConnectionAccepted)
    val subAck = streaming.SubAck(streaming.PacketId(1), List(streaming.ControlPacketFlags.QoSAtLeastOnceDelivery))

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
            .wireTap(Sink.foreach[Either[DecodeError, streaming.Event[_]]] {
              case Right(streaming.Event(_: streaming.Connect, _)) =>
                server.offer(streaming.Command(connAck))
              case Right(streaming.Event(s: streaming.Subscribe, _)) =>
                server.offer(streaming.Command(subAck.copy(packetId = s.packetId)))
              case Right(streaming.Event(_: streaming.PubAck, _)) =>
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

    Source
      .fromGraph(clientSource)
      .via(
        MqttFlow.atLeastOnce(
          connectionSettings,
          MqttSubscriptions("some-topic", MqttQoS.AtLeastOnce),
          bufferSize = 8,
          MqttQoS.AtLeastOnce
        )
      )
      .mapAsync(1)(_.ack())
      .runWith(Sink.ignore)
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
