/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.scaladsl

import java.io.File

import akka.stream.alpakka.avroparquet.scaladsl.AvroParquetSink
import akka.stream.scaladsl.{Sink, Source}
import akka.stream.testkit.scaladsl.StreamTestKit.assertAllStagesStopped
import akka.testkit.TestKit
import org.scalatest.concurrent.ScalaFutures

import scala.reflect.io.Directory
import org.apache.avro.generic.GenericRecord
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

class AvroParquetSinkSpec
    extends AnyWordSpecLike
    with Matchers
    with AbstractAvroParquet
    with ScalaFutures
    with BeforeAndAfterAll {

  "Parquet Sink" should {

    "create new parquet file" in assertAllStagesStopped {
      //given
      val n: Int = 3
      val records: List[GenericRecord] = genDocuments(n).sample.get.map(docToRecord)
      Source
        .fromIterator(() => records.iterator)
        .runWith(AvroParquetSink(parquetWriter(file, conf, schema)))
        .futureValue

      //when
      val parquetContent: List[GenericRecord] = fromParquet[GenericRecord](file, conf)

      //then
      parquetContent.length shouldEqual n
      parquetContent should contain theSameElementsAs records
    }

  }

  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
    val directory = new Directory(new File(folder))
    directory.deleteRecursively()
  }

}
