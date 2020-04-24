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
import org.scalatest.concurrent.ScalaFutures
import org.apache.avro.generic.GenericRecord
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

    "create new parquet file" in assertAllStagesStopped {
      //given
      val n: Int = 3
      val records: List[GenericRecord] = genDocuments(n).sample.get.map(docToRecord)
      // #init-sink
      val source: Source[GenericRecord, NotUsed] = // ???
        // #init-sink
        Source(records)
      val writer = parquetWriter(file, conf, schema)
      // #init-sink
      val result: Future[Done] = source
        .runWith(AvroParquetSink(writer))
      // #init-sink
      result.futureValue shouldBe Done

      //when
      val parquetContent: List[GenericRecord] = fromParquet(file, conf)

      //then
      parquetContent.length shouldEqual n
      parquetContent should contain theSameElementsAs records
    }

  }

}
