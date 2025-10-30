/*
 * Copyright (C) since 2016 Lightbend Inc. <https://akka.io>
 */

package docs.scaladsl

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.alpakka.avroparquet.scaladsl.AvroParquetFlow
import akka.stream.scaladsl.{Flow, Sink, Source}
import akka.stream.testkit.scaladsl.StreamTestKit.assertAllStagesStopped
import akka.testkit.TestKit
import com.sksamuel.avro4s.Record
import org.apache.avro.generic.GenericRecord
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike
import org.apache.parquet.hadoop.ParquetWriter
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.ScalaFutures

class AvroParquetFlowSpec
    extends TestKit(ActorSystem("FlowSpec"))
    with AnyWordSpecLike
    with Matchers
    with AbstractAvroParquet
    with ScalaFutures
    with BeforeAndAfterAll {

  "Parquet Flow" should {

    "insert avro records in parquet from `GenericRecord`" in assertAllStagesStopped {
      //given
      val n: Int = 2
      val file: String = genFinalFile.sample.get
      // #init-flow
      val records: List[GenericRecord]
      // #init-flow
      = genDocuments(n).sample.get.map(docToGenericRecord)
      val writer: ParquetWriter[GenericRecord] = parquetWriter(file, conf, schema)

      //when
      // #init-flow
      val source: Source[GenericRecord, NotUsed] = Source(records)
      val avroParquet: Flow[GenericRecord, GenericRecord, NotUsed] = AvroParquetFlow(writer)
      val result =
        source
          .via(avroParquet)
          .runWith(Sink.seq)
      // #init-flow

      result.futureValue

      //then
      val parquetContent: List[GenericRecord] = fromParquet(file, conf)
      parquetContent.length shouldEqual n
      parquetContent should contain theSameElementsAs records
    }

    "insert avro records in parquet from a subtype of `GenericRecord`" in assertAllStagesStopped {
      //given
      val n: Int = 2
      val file: String = genFinalFile.sample.get
      val documents: List[Document] = genDocuments(n).sample.get
      val avroDocuments: List[Record] = documents.map(format.to(_))
      val writer: ParquetWriter[Record] = parquetWriter[Record](file, conf, schema)

      //when
      Source(avroDocuments)
        .via(AvroParquetFlow[Record](writer))
        .runWith(Sink.seq)
        .futureValue

      //then
      val parquetContent: List[GenericRecord] = fromParquet(file, conf)
      parquetContent.length shouldEqual n
      parquetContent.map(format.from(_)) should contain theSameElementsAs documents
    }
  }

}
