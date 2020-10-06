/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.pravega

import scala.language.postfixOps

import akka.stream.scaladsl.{Keep, Sink, Source}
import io.pravega.client.stream.impl.UTF8StringSerializer

import scala.concurrent.Await
import scala.concurrent.duration.DurationInt

import io.pravega.client.stream.Serializer

import java.nio.ByteBuffer
import akka.stream.alpakka.testkit.scaladsl.Repeated
import akka.stream.alpakka.pravega.scaladsl.PravegaTable

case class Person(id: Int, firstname: String)

class PravegaKVTableSpec extends PravegaBaseSpec with Repeated {

  private val serializer = new UTF8StringSerializer

  private val intSerializer = new Serializer[Int] {
    override def serialize(value: Int): ByteBuffer = {
      val buff = ByteBuffer.allocate(4).putInt(value)
      buff.position(0)
      buff
    }

    override def deserialize(serializedValue: ByteBuffer): Int =
      serializedValue.getInt
  }

  private implicit val tablewriterSettings: TableWriterSettings[Int, String] =
    TableWriterSettingsBuilder[Int, String](system)
      .withSerializers(intSerializer, serializer)

  val familyExtractor: Int => String =
    k => if (k % 2 == 0) "test" else null

  "Pravega connector" should {

    "write and read in KVP table" in {

      val scope = newScope()

      val tableName = "kvp-table-name"

      createTable(scope, tableName)

      val sink = PravegaTable.sink(scope, tableName, familyExtractor)

      val fut = Source(1 to 100)
        .map(id => (id, s"name_$id"))
        .runWith(sink)

      Await.ready(fut, 10 seconds)

      val tableSettings = TableSettingsBuilder
        .apply[Int, String](system.settings.config.getConfig(TableSettingsBuilder.configPath))
        .withKVSerializers(intSerializer, serializer)

      val readingDone = PravegaTable
        .source(scope, tableName, "test", tableSettings)
        .toMat(Sink.fold(0) { (sum, value) =>
          sum + 1
        })(Keep.right)
        .run()

      whenReady(readingDone) { sum =>
        logger.info(s"Sum: $sum")
        sum mustEqual 50
      }

    }

  }

}
