/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.scaladsl

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.alpakka.avroparquet.scaladsl.{AvroParquetSink, AvroParquetSource}
import akka.stream.scaladsl.{Keep, Source}
import akka.stream.testkit.scaladsl.StreamTestKit.assertAllStagesStopped
import akka.stream.testkit.scaladsl.TestSink
import akka.testkit.TestKit
import com.sksamuel.avro4s.Record
import org.scalatest.concurrent.ScalaFutures
import org.apache.avro.generic.GenericRecord
import org.apache.parquet.hadoop.ParquetReader
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

import scala.concurrent.duration._

class AvroParquetSourceSpec
    extends TestKit(ActorSystem("SourceSpec"))
    with AnyWordSpecLike
    with AbstractAvroParquet
    with Matchers
    with ScalaFutures
    with BeforeAndAfterAll {

  "AvroParquetSource" should {

    "read from parquet file as a `GenericRecord` type" in assertAllStagesStopped {
      //given
      val n: Int = 4
      val file: String = genFinalFile.sample.get
      val records: List[GenericRecord] = genDocuments(n).sample.get.map(docToGenericRecord)
      Source(records)
        .toMat(AvroParquetSink(parquetWriter(file, conf, schema)))(Keep.right)
        .run()
        .futureValue

      //when
      val reader: ParquetReader[GenericRecord] = parquetReader(file, conf)
      // #init-source
      val source: Source[GenericRecord, NotUsed] = AvroParquetSource(reader)
      // #init-source
      val sink = source.runWith(TestSink.probe)

      //then
      val result: Seq[GenericRecord] = sink.toStrict(3.seconds)
      result.length shouldEqual n
      result should contain theSameElementsAs records
    }

    "read from parquet file as any subtype of `GenericRecord` " in assertAllStagesStopped {
      //given
      val n: Int = 4
      val file: String = genFinalFile.sample.get
      val documents: List[Document] = genDocuments(n).sample.get
      val avroDocuments: List[Record] = documents.map(format.to(_))
      Source(avroDocuments)
        .toMat(AvroParquetSink(parquetWriter(file, conf, schema)))(Keep.right)
        .run()
        .futureValue

      //when
      val reader: ParquetReader[GenericRecord] = parquetReader(file, conf)
      // #init-source
      val source: Source[GenericRecord, NotUsed] = AvroParquetSource(reader)
      // #init-source
      val sink = source.runWith(TestSink.probe)

      //then
      val result: Seq[GenericRecord] = sink.toStrict(3.seconds)
      result.length shouldEqual n
      result.map(format.from(_)) should contain theSameElementsAs documents
    }

  }

}
