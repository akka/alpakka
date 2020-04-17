/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.scaladsl

import java.io.File
import java.util.concurrent.TimeUnit

import akka.Done
import akka.stream.alpakka.avroparquet.scaladsl.AvroParquetSink
import akka.stream.scaladsl.{Sink, Source}
import akka.stream.testkit.scaladsl.StreamTestKit.assertAllStagesStopped
import akka.testkit.TestKit
import org.apache.parquet.avro.AvroParquetReader
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.should.Matchers
import org.specs2.mutable.Specification
import org.specs2.specification.AfterAll

import scala.collection.{immutable, mutable}
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.reflect.io.Directory
//#init-writer
import org.apache.hadoop.conf.Configuration
import org.apache.parquet.hadoop.ParquetWriter
import org.apache.avro.generic.GenericRecord
import org.apache.hadoop.fs.Path
import org.apache.parquet.hadoop.util.HadoopInputFile
import org.apache.parquet.avro.{AvroParquetWriter, AvroReadSupport}
//#init-writer

class AvroParquetSinkSpec extends Specification with AbstractAvroParquet with ScalaFutures with AfterAll {

  "Parquet Sink" should {

    "create new parquet file" in assertAllStagesStopped {
      val initialRecords: List[GenericRecord] = genDocuments.sample.get.map(docToRecord)

      Source
        .fromIterator(() => initialRecords.iterator)
        .runWith(AvroParquetSink(parquetWriter(file, conf, schema)))
        .futureValue

      val parquetContent: List[GenericRecord] = fromParquet[GenericRecord](file, conf)
      parquetContent.length shouldEqual 3

    }

  }

  def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
    val directory = new Directory(new File(folder))
    directory.deleteRecursively()
  }

}
