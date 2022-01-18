/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.pravega

import akka.stream.scaladsl.{Keep, Sink, Source}

import scala.concurrent.Await

import akka.stream.alpakka.testkit.scaladsl.Repeated
import akka.stream.alpakka.pravega.scaladsl.PravegaTable
import io.pravega.client.tables.TableKey

import docs.scaladsl.Person
import docs.scaladsl.Serializers._

class PravegaKVTableSpec extends PravegaBaseSpec with Repeated {

  private val tablewriterSettings: TableWriterSettings[Int, Person] =
    TableWriterSettingsBuilder[Int, Person]()
      .withSerializers(id => new TableKey(intSerializer.serialize(id)))
      .build()

  "Pravega connector" should {

    "write and read in KVP table with keyFamily = \"test\" " in {

      val scope = newScope()

      val tableName = "kvp-table-name"

      createTable(scope, tableName, 4)

      val sink = PravegaTable.sink(scope, tableName, tablewriterSettings)

      val fut = Source(1 to 100)
        .map(id => (id, Person(id, s"name_$id")))
        .runWith(sink)

      Await.ready(fut, remainingOrDefault)

      val tableSettings = TableReaderSettingsBuilder[Int, Person]()
        .withTableKey(id => new TableKey(intSerializer.serialize(id)))
        .build()

      val readingDone = PravegaTable
        .source(scope, tableName, tableSettings)
        .toMat(Sink.fold(0) { (sum, value) =>
          sum + 1
        })(Keep.right)
        .run()

      whenReady(readingDone) { sum =>
        logger.info(s"Sum: $sum")
        sum mustEqual 100
      }

    }

  }

}
