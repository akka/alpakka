/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.hbase.scaladsl

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.alpakka.hbase.HTableSettings
import akka.stream.alpakka.hbase.Utils.{HBaseMockConfiguration, MockHTable}
import akka.stream.scaladsl.{Sink, Source}
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util.Bytes
import org.scalatest.{Matchers, WordSpec}

import scala.collection.immutable
import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.language.implicitConversions
import scala.util.{Failure, Success}

class HBaseStageSpec extends WordSpec with Matchers {

  implicit def toBytes(string: String): Array[Byte] = Bytes.toBytes(string)

  case class Person(id: Int, name: String)

  val hbaseIT: Option[String] = sys.env.get("HBASE_TEST")

  //#create-converter
  val hBaseConverter: Person => Put = { person =>
    val put = new Put(s"id_${person.id}")
    put.addColumn("info", "name", person.name)
    put
  }

  val hBaseMultiConverter: Person => immutable.Seq[Put] = { person =>
    val put = new Put(s"id_${person.id}")
    put.addColumn("info", "name", person.name)
    List(put)
  }
  //#create-converter

  //#create-settings
  val table = TableName.valueOf("person")
  private val configuration = HBaseMockConfiguration.create(new MockHTable(table))
  val tableSettings =
    HTableSettings(configuration, table, immutable.Seq("info"), hBaseConverter)
  //#create-settings
  hbaseIT.foreach { hbase =>
    "HBase stages " must {

      "sinks in hbase" in {
        implicit val actorSystem = ActorSystem("reactiveStreams")
        implicit val materilizer = ActorMaterializer()

        //#sink
        val sink = HTableStage.sink[Person](tableSettings)

        val f = Source(1 to 10).map(i => Person(i, s"zozo_$i")).runWith(sink)
        //#sink

        f.onComplete {
          case e =>
            actorSystem.terminate()
        }

        Await.ready(f, Duration.Inf)

      }

      "sinks in hbase with multi put" in {
        implicit val actorSystem = ActorSystem("reactiveStreams")
        implicit val materilizer = ActorMaterializer()

        //#sink
        val sink = HTableStage.sink[Person](HTableSettings(configuration, table, immutable.Seq("info"), hBaseMultiConverter))

        val f = Source(1 to 10).map(i => Person(i, s"zozo_$i")).runWith(sink)
        //#sink

        f.onComplete {
          case e =>
            actorSystem.terminate()
        }

        Await.ready(f, Duration.Inf)
      }

      "flows through hbase" in {
        implicit val actorSystem = ActorSystem("reactiveStreams")
        implicit val materilizer = ActorMaterializer()

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

      "flows through hbase with multi put" in {
        implicit val actorSystem = ActorSystem("reactiveStreams")
        implicit val materilizer = ActorMaterializer()

        //#flow
        val flow = HTableStage.flow[Person](HTableSettings(configuration, table, immutable.Seq("info"), hBaseMultiConverter))

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
