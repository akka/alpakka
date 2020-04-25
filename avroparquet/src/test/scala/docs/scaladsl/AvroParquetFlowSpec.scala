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
import com.sksamuel.avro4s.Record
import org.apache.avro.generic.GenericRecord
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike
import org.apache.parquet.hadoop.ParquetWriter
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.ScalaFutures

import scala.collection.immutable
import scala.concurrent.Future

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
      val records: List[Record] = genDocuments(n).sample.get.map(format.to(_))
      val writer: ParquetWriter[Record] = avro4sWriter[Record](file, conf, schema)

      //when
      // #init-flow
      val source: Source[Record, NotUsed] = // ???
        // #init-flow
        Source(records)
      // #init-flow

      val avroParquet: Flow[Record, Record, NotUsed] = AvroParquetFlow[Record](writer)
      val result: Future[immutable.Seq[Record]] =
        source
          .via(avroParquet)
          .runWith(Sink.seq)
      // #init-flow

      result.futureValue

      //then
      val parquetContent: List[GenericRecord] = fromParquet(file, conf)
      parquetContent.length shouldEqual n
      parquetContent.map(format.from(_)) should contain theSameElementsAs records.map(format.from(_))
    }
  }

}
