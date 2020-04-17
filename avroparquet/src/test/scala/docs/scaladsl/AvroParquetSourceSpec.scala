/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.scaladsl

import java.io.File

import akka.actor.ActorSystem
import akka.stream.alpakka.avroparquet.scaladsl.{AvroParquetSink, AvroParquetSource}
import akka.stream.scaladsl.{Keep, Source}
import akka.stream.testkit.scaladsl.StreamTestKit.assertAllStagesStopped
import akka.stream.testkit.scaladsl.TestSink
import akka.testkit.TestKit
import org.scalatest.concurrent.ScalaFutures
import org.apache.avro.generic.GenericRecord
import org.apache.parquet.hadoop.ParquetReader
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

import scala.concurrent.duration._
import scala.reflect.io.Directory

class AvroParquetSourceSpec
    extends TestKit(ActorSystem("SourceSpec"))
    with AnyWordSpecLike
    with AbstractAvroParquet
    with Matchers
    with ScalaFutures
    with BeforeAndAfterAll {

  "AvroParquetSource" should {

    "read from parquet file" in assertAllStagesStopped {
      //given
      val n: Int = 4
      val records: List[GenericRecord] = genDocuments(n).sample.get.map(docToRecord)
      Source
        .fromIterator(() => records.iterator)
        .toMat(AvroParquetSink(parquetWriter(file, conf, schema)))(Keep.right)
        .run()
        .futureValue

      //when
      val reader: ParquetReader[GenericRecord] = parquetReader(file, conf)
      val (_, sink) = AvroParquetSource(reader)
        .toMat(TestSink.probe)(Keep.both)
        .run()

      //then
      val result: Seq[GenericRecord] = sink.toStrict(3.seconds)
      result.length shouldEqual n
      result should contain theSameElementsAs records
    }

  }

  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
    val directory = new Directory(new File(folder))
    directory.deleteRecursively()
  }

}
