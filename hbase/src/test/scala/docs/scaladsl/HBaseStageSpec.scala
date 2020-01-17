/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package docs.scaladsl

import akka.Done
import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.alpakka.hbase.HTableSettings
import akka.stream.alpakka.hbase.scaladsl.HTableStage
import akka.stream.alpakka.testkit.scaladsl.LogCapturing
import akka.stream.scaladsl.{Sink, Source}
import akka.testkit.TestKit
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.BeforeAndAfterAll

import scala.collection.immutable
import scala.concurrent.duration._
import scala.language.implicitConversions
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

class HBaseStageSpec
    extends TestKit(ActorSystem("HBaseStageSpec"))
    with AnyWordSpecLike
    with Matchers
    with ScalaFutures
    with BeforeAndAfterAll
    with LogCapturing {

  implicit val mat = ActorMaterializer()

  implicit val defaultPatience =
    PatienceConfig(timeout = 5.seconds, interval = 500.millis)

  //#create-converter-put
  implicit def toBytes(string: String): Array[Byte] = Bytes.toBytes(string)
  case class Person(id: Int, name: String)

  val hBaseConverter: Person => immutable.Seq[Mutation] = { person =>
    val put = new Put(s"id_${person.id}")
    put.addColumn("info", "name", person.name)
    List(put)
  }
  //#create-converter-put

  //#create-converter-append
  val appendHBaseConverter: Person => immutable.Seq[Mutation] = { person =>
    // Append to a cell
    val append = new Append(s"id_${person.id}")
    append.add("info", "aliases", person.name)
    List(append)
  }
  //#create-converter-append

  //#create-converter-delete
  val deleteHBaseConverter: Person => immutable.Seq[Mutation] = { person =>
    // Delete the specified row
    val delete = new Delete(s"id_${person.id}")
    List(delete)
  }
  //#create-converter-delete

  //#create-converter-increment
  val incrementHBaseConverter: Person => immutable.Seq[Mutation] = { person =>
    // Increment a cell value
    val increment = new Increment(s"id_${person.id}")
    increment.addColumn("info", "numberOfChanges", 1)
    List(increment)
  }
  //#create-converter-increment

  //#create-converter-complex
  val mutationsHBaseConverter: Person => immutable.Seq[Mutation] = { person =>
    if (person.id != 0) {
      if (person.name.isEmpty) {
        // Delete the specified row
        val delete = new Delete(s"id_${person.id}")
        List(delete)
      } else {
        // Insert or update a row
        val put = new Put(s"id_${person.id}")
        put.addColumn("info", "name", person.name)

        val increment = new Increment(s"id_${person.id}")
        increment.addColumn("info", "numberOfChanges", 1)

        List(put, increment)
      }
    } else {
      List.empty
    }
  }
  //#create-converter-complex

  //#create-settings
  val tableSettings =
    HTableSettings(HBaseConfiguration.create(), TableName.valueOf("person"), immutable.Seq("info"), hBaseConverter)
  //#create-settings

  "HBase stage" must {

    "write write entries to a sink" in {
      //#sink
      val sink = HTableStage.sink[Person](tableSettings)

      val f = Source(1 to 10).map(i => Person(i, s"zozo_$i")).runWith(sink)
      //#sink

      f.futureValue shouldBe Done
    }

    "write entries through a flow" in {
      //#flow
      val flow = HTableStage.flow[Person](tableSettings)

      val f = Source(11 to 20).map(i => Person(i, s"zozo_$i")).via(flow).runWith(Sink.fold(0)((a, d) => a + d.id))
      //#flow

      f.futureValue shouldBe 155
    }

    "scan entries from a source" in {
      val create = Source(List(Person(100, "scan_100"))).runWith(HTableStage.sink(tableSettings))
      create.futureValue shouldBe Done

      //#source
      val scan = new Scan(new Get(Bytes.toBytes("id_100")))

      val f = HTableStage
        .source(scan, tableSettings)
        .runWith(Sink.seq)
      //#source

      f.futureValue.size shouldBe 1
    }
  }

  override def afterAll(): Unit =
    TestKit.shutdownActorSystem(system)
}
