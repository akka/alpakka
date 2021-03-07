/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.scaladsl

import java.time.Instant
import java.util.concurrent.TimeUnit

import akka.Done
import akka.actor.ActorSystem
import akka.stream.alpakka.influxdb.{InfluxDbReadSettings, InfluxDbWriteMessage, InfluxDbWriteResult}
import akka.stream.alpakka.influxdb.scaladsl.{InfluxDbFlow, InfluxDbSource}
import akka.stream.alpakka.testkit.scaladsl.LogCapturing
import akka.stream.scaladsl.{Sink, Source}
import akka.testkit.TestKit
import org.influxdb.InfluxDB
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
import org.scalatest.concurrent.ScalaFutures
import docs.javadsl.TestUtils._
import akka.stream.testkit.scaladsl.StreamTestKit.assertAllStagesStopped
import org.influxdb.dto.{Point, Query}

import scala.concurrent.duration._
import org.scalatest.matchers.must.Matchers
import org.scalatest.wordspec.AnyWordSpec

class FlowSpec
    extends AnyWordSpec
    with Matchers
    with BeforeAndAfterEach
    with BeforeAndAfterAll
    with ScalaFutures
    with LogCapturing {

  implicit val system = ActorSystem()

  final val DatabaseName = this.getClass.getSimpleName

  implicit var influxDB: InfluxDB = _

  override protected def beforeAll(): Unit =
    influxDB = setupConnection(DatabaseName)

  override protected def afterAll(): Unit = {
    dropDatabase(influxDB, DatabaseName)
    TestKit.shutdownActorSystem(system)
  }

  "mixed model" in assertAllStagesStopped {
    val point = Point
      .measurement("disk")
      .time(System.currentTimeMillis(), TimeUnit.MILLISECONDS)
      .addField("used", 80L)
      .addField("free", 1L)
      .build()

    val validMessage = InfluxDbWriteMessage(point)
      .withDatabaseName(DatabaseName)

    //#run-flow
    val result = Source(
      List(
        List(validMessage)
      )
    ).via(InfluxDbFlow.create())
      .runWith(Sink.seq)
      .futureValue
    //#run-flow

    result(0)(0).error mustBe None
  }

  "kafka-example - store metrics and pass Responses with passThrough" in assertAllStagesStopped {
    //#kafka-example
    // We're going to pretend we got messages from kafka.
    // After we've written them to InfluxDB, we want
    // to commit the offset to Kafka

    case class KafkaOffset(offset: Int)
    case class KafkaMessage(cpu: InfluxDbFlowCpu, offset: KafkaOffset)

    val messagesFromKafka = List(
      KafkaMessage(new InfluxDbFlowCpu(Instant.now().minusSeconds(1000), "local_1", "eu-west-2", 1.4d, true, 123L),
                   KafkaOffset(0)),
      KafkaMessage(new InfluxDbFlowCpu(Instant.now().minusSeconds(2000), "local_2", "eu-west-1", 2.5d, false, 125L),
                   KafkaOffset(1)),
      KafkaMessage(new InfluxDbFlowCpu(Instant.now().minusSeconds(3000), "local_3", "eu-west-4", 3.1d, false, 251L),
                   KafkaOffset(2))
    )

    var committedOffsets = List[KafkaOffset]()

    def commitToKafka(offset: KafkaOffset): Unit =
      committedOffsets = committedOffsets :+ offset

    val f1 = Source(messagesFromKafka)
      .map { kafkaMessage: KafkaMessage =>
        val cpu = kafkaMessage.cpu
        println("hostname: " + cpu.getHostname)

        InfluxDbWriteMessage(cpu).withPassThrough(kafkaMessage.offset)
      }
      .groupedWithin(10, 50.millis)
      .via(
        InfluxDbFlow.typedWithPassThrough(classOf[InfluxDbFlowCpu])
      )
      .map { messages: Seq[InfluxDbWriteResult[InfluxDbFlowCpu, KafkaOffset]] =>
        messages.foreach { message =>
          commitToKafka(message.writeMessage.passThrough)
        }
      }
      .runWith(Sink.ignore)

    //#kafka-example
    f1.futureValue mustBe Done
    assert(List(0, 1, 2) == committedOffsets.map(_.offset))

    val f2 = InfluxDbSource
      .typed(classOf[InfluxDbFlowCpu], InfluxDbReadSettings.Default, influxDB, new Query("SELECT*FROM cpu"))
      .map { cpu =>
        cpu.getHostname
      }
      .runWith(Sink.seq)

    f2.futureValue.sorted mustBe Seq(
      "local_1",
      "local_2",
      "local_3"
    )
  }

}
