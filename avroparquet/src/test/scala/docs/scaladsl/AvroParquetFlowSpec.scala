/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.scaladsl

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.alpakka.avroparquet.scaladsl.AvroParquetFlow
import akka.stream.scaladsl.{Flow, Sink, Source}
import akka.stream.testkit.scaladsl.StreamTestKit.assertAllStagesStopped
import akka.testkit.TestKit
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

    "insert avro records in parquet" in assertAllStagesStopped {
      //given
      val n: Int = 2
      val records: List[GenericRecord] = genDocuments(n).sample.get.map(docToRecord)
      val writer: ParquetWriter[GenericRecord] = parquetWriter(file, conf, schema)

      //when
      // #init-flow
      val source: Source[GenericRecord, NotUsed] = // ???
        // #init-flow
        Source.fromIterator(() => records.iterator)
      // #init-flow

      val avroParquet: Flow[GenericRecord, GenericRecord, NotUsed] = AvroParquetFlow(writer)
      val result =
        source
          .via(avroParquet)
          .runWith(Sink.seq)
          // #init-flow
          .futureValue

      //then
      val parquetContent: List[GenericRecord] = fromParquet(file, conf)
      parquetContent.length shouldEqual n
      parquetContent should contain theSameElementsAs records
    }

  }

}
