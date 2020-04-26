/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.scaladsl

import akka.{Done, NotUsed}
import akka.actor.ActorSystem
import akka.stream.alpakka.avroparquet.scaladsl.AvroParquetSink
import akka.stream.scaladsl.Source
import akka.stream.testkit.scaladsl.StreamTestKit.assertAllStagesStopped
import akka.testkit.TestKit
import com.sksamuel.avro4s.Record
import org.scalatest.concurrent.ScalaFutures
import org.apache.avro.generic.GenericRecord
import org.apache.parquet.hadoop.ParquetWriter
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

import scala.concurrent.Future

class AvroParquetSinkSpec
    extends TestKit(ActorSystem("SinkSpec"))
    with AnyWordSpecLike
    with Matchers
    with AbstractAvroParquet
    with ScalaFutures
    with BeforeAndAfterAll {

  "Parquet Sink" should {

    "create new parquet file from `GenericRecords`" in assertAllStagesStopped {
      //given
      val n: Int = 3
      val file: String = genFinalFile.sample.get
      val records: List[GenericRecord] = genDocuments(n).sample.get.map(docToGenericRecord)

      val writer = parquetWriter(file, conf, schema)
      Source(records).runWith(AvroParquetSink(writer)).futureValue

      //when
      val parquetContent: List[GenericRecord] = fromParquet(file, conf)

      //then
      parquetContent.length shouldEqual n
      parquetContent should contain theSameElementsAs records
    }

    "create new parquet file from any subtype of `GenericRecord` " in assertAllStagesStopped {
      //given
      val n: Int = 3
      val file: String = genFinalFile.sample.get
      val documents: List[Document] = genDocuments(n).sample.get
      // #init-sink
      val records: List[Record] = // ???
        documents.map(format.to(_))
      // #init-sink
      val source: Source[Record, NotUsed] = Source(records)
      // #init-sink
      val writer: ParquetWriter[Record] = avro4sWriter[Record](file, conf, schema)
      // #init-sink
      val result: Future[Done] = source
        .runWith(AvroParquetSink(writer))
      // #init-sink
      result.futureValue shouldBe Done

      //when
      val parquetContent: List[GenericRecord] = fromParquet(file, conf)

      //then
      parquetContent.length shouldEqual n
      parquetContent.map(format.from(_)) should contain theSameElementsAs documents
    }

  }

}
