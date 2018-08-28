/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.hbase.scaladsl

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.alpakka.hbase.HTableSettings
import akka.stream.scaladsl.{Sink, Source}
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.scalatest.{Matchers, WordSpec}

import scala.collection.immutable
import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.language.implicitConversions
import scala.util.{Failure, Success}

class HBaseStageSpec extends WordSpec with Matchers {

  val hbaseIT: Option[String] = sys.env.get("HBASE_TEST")

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
  hbaseIT.foreach { hbase =>
    "HBase stages " must {

      "sinks in hbase" in {
        implicit val actorSystem: ActorSystem = ActorSystem("reactiveStreams")
        implicit val materilizer: ActorMaterializer = ActorMaterializer()

        //#sink
        val sink = HTableStage.sink[Person](tableSettings)

        val f = Source(1 to 10).map(i => Person(i, s"zozo_$i")).runWith(sink)
        //#sink

        f.onComplete(e => actorSystem.terminate())

        Await.ready(f, Duration.Inf)

      }

      "flows through hbase" in {
        implicit val actorSystem: ActorSystem = ActorSystem("reactiveStreams")
        implicit val materilizer: ActorMaterializer = ActorMaterializer()

        //#flow
        val flow = HTableStage.flow[Person](tableSettings)

        val f = Source(11 to 20).map(i => Person(i, s"zozo_$i")).via(flow).runWith(Sink.fold(0)((a, d) => a + d.id))
        //#flow

        f.onComplete {
          case Success(sum) =>
            println(s"id sums: $sum")
            actorSystem.terminate()
          case Failure(e) =>
            actorSystem.terminate()
        }

        Await.ready(f, Duration.Inf)
      }
    }

  }
}
