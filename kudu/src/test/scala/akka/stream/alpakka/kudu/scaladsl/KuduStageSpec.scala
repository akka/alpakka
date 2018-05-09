/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.kudu.scaladsl

import java.util

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.alpakka.kudu.KuduTableSettings
import akka.stream.scaladsl.{Sink, Source}
import org.apache.kudu.{ColumnSchema, Schema, Type}
import org.apache.kudu.client.{CreateTableOptions, KuduClient, PartialRow}
import org.scalatest.{Matchers, WordSpec}

import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.util.{Failure, Success}

class KuduStageSpec extends WordSpec with Matchers {

  //#create-converter
  case class Person(id: Int, name: String)

  val cols = new util.ArrayList[ColumnSchema]()
  cols.add(new ColumnSchema.ColumnSchemaBuilder("key", Type.INT32).key(true).build)
  cols.add(new ColumnSchema.ColumnSchemaBuilder("value", Type.STRING).build)
  val schema = new Schema(cols)

  val kuduConverter: Person => PartialRow = { person =>
    val partialRow = schema.newPartialRow()
    partialRow.addInt(0, person.id)
    partialRow.addString(1, person.name)
    partialRow
  }
  //#create-converter

  //#kudu connection and table creation steps
  //#create-settings
  val rangeKeys: util.List[String] = new util.ArrayList[String]
  rangeKeys.add("key")
  val createTableOptions = new CreateTableOptions().setNumReplicas(1).setRangePartitionColumns(rangeKeys)
  //#create-settings

  "Kudu stages " must {

    "sinks in kudu" in {
      implicit val actorSystem = ActorSystem("reactiveStreams")
      implicit val materilizer = ActorMaterializer()

      //#create-settings
      val kuduClient = new KuduClient.KuduClientBuilder("localhost:7051").build
      val kuduTableSettings = KuduTableSettings(kuduClient, "test", schema, createTableOptions, kuduConverter)
      //#create-settings

      //#sink
      val sink = KuduTableStage.sink[Person](kuduTableSettings)

      val f = Source(1 to 10).map(i => Person(i, s"zozo_$i")).runWith(sink)
      //#sink

      f.onComplete {
        case e =>
          actorSystem.terminate()
      }

      Await.ready(f, Duration.Inf)
    }

    "flows through kudu" in {
      implicit val actorSystem = ActorSystem("reactiveStreams")
      implicit val materilizer = ActorMaterializer()

      val kuduClient = new KuduClient.KuduClientBuilder("localhost:7051").build
      val kuduTableSettings = KuduTableSettings(kuduClient, "test", schema, createTableOptions, kuduConverter)

      //#flow
      val flow = KuduTableStage.flow[Person](kuduTableSettings)

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
