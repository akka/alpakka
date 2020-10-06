/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.pravega

import scala.language.postfixOps
import akka.stream.alpakka.pravega.scaladsl.Pravega
import akka.stream.scaladsl.{Keep, Sink, Source}
import io.pravega.client.stream.impl.UTF8StringSerializer

import scala.concurrent.Await
import scala.concurrent.duration.DurationInt

import io.pravega.client.stream.Serializer

import java.nio.ByteBuffer
import akka.stream.alpakka.testkit.scaladsl.Repeated

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

  val familyExtractor: Person => String = p => if (p.id % 2 == 0) "test" else null

  "Pravega connector" should {

    "write and read in KVP table" in {

      val fut = Source(1 to 100)
        .map(id => Person(id = id, firstname = s"name_$id"))
        .runWith(Pravega.tableSink(scope, keyValueTableName, (p: Person) => (p.id, p.firstname), familyExtractor))

      Await.ready(fut, 10 seconds)

      val tableSettings = TableSettingsBuilder
        .apply[Int, String](system.settings.config.getConfig(TableSettingsBuilder.configPath))
        .withKVSerializers(intSerializer, serializer)

      val readingDone = Pravega
        .tableSource(scope, keyValueTableName, "test", tableSettings)
        .toMat(Sink.fold(0) { (sum, value) =>
          sum + value._1
        })(Keep.right)
        .run()

      whenReady(readingDone) { sum =>
        logger.info(s"Sum: $sum")
        sum mustEqual 2550
      }

    }

  }

}
