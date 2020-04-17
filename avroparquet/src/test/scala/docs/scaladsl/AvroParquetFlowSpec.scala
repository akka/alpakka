/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.scaladsl

import akka.stream.alpakka.avroparquet.scaladsl.AvroParquetFlow
import akka.stream.scaladsl.{Sink, Source}
import akka.stream.testkit.scaladsl.StreamTestKit.assertAllStagesStopped
import org.apache.avro.generic.GenericRecord
import org.apache.hadoop.fs.Path
import org.apache.parquet.avro.AvroParquetWriter
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike
import org.apache.parquet.hadoop.ParquetWriter
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.ScalaFutures

import scala.concurrent.duration._

class AvroParquetFlowSpec
    extends AnyWordSpecLike
    with Matchers
    with AbstractAvroParquet
    with ScalaFutures
    with BeforeAndAfterAll {

  override implicit val patienceConfig = PatienceConfig(5.seconds, 50.milliseconds)

  "Parquet Flow" should {

    "insert avro records in parquet" in assertAllStagesStopped {
      //given
      val initialRecords: List[GenericRecord] = genDocuments.sample.get.map(docToRecord)
      val writer: ParquetWriter[GenericRecord] = parquetWriter(file, conf, schema)

      //when
      Source
        .fromIterator(() => initialRecords.iterator)
        .via(AvroParquetFlow(writer))
        .runWith(Sink.ignore)
        .futureValue

      //then
      val parquetContent: List[GenericRecord] = fromParquet[GenericRecord](file, conf)
      parquetContent.length shouldEqual 3
      parquetContent should contain theSameElementsAs initialRecords
    }

  }
}
