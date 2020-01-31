/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.scaladsl

import java.util.concurrent.TimeUnit

import akka.Done
import akka.stream.alpakka.avroparquet.scaladsl.AvroParquetSink
import akka.stream.scaladsl.{Sink, Source}
import akka.stream.testkit.scaladsl.StreamTestKit.assertAllStagesStopped
import org.apache.parquet.avro.AvroParquetReader
import org.specs2.mutable.Specification
import org.specs2.specification.AfterAll

import scala.collection.mutable
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
//#init-writer
import org.apache.hadoop.conf.Configuration
import org.apache.parquet.hadoop.ParquetWriter
import org.apache.avro.generic.GenericRecord
import org.apache.hadoop.fs.Path
import org.apache.parquet.hadoop.util.HadoopInputFile
import org.apache.parquet.avro.{AvroParquetWriter, AvroReadSupport}
//#init-writer

class AvroParquetSinkSpec extends Specification with AbstractAvroParquet with AfterAll {

  "ParquetSing Sink" should {

    "create new Parquet file" in assertAllStagesStopped {
      val docs = List[Document](Document("id1", "sdaada"), Document("id1", "sdaada"), Document("id3", " fvrfecefedfww"))

      val source = Source.fromIterator(() => docs.iterator)

      //#init-writer
      val file = folder + "/test.parquet"

      val conf = new Configuration()
      conf.setBoolean(AvroReadSupport.AVRO_COMPATIBILITY, true)

      val writer: ParquetWriter[GenericRecord] =
        AvroParquetWriter.builder[GenericRecord](new Path(file)).withConf(conf).withSchema(schema).build()
      //#init-writer

      //#init-sink
      val sink: Sink[GenericRecord, Future[Done]] = AvroParquetSink(writer)
      //#init-sink

      val result: Future[Done] = source
        .map(docToRecord)
        .runWith(sink)

      Await.result[Done](result, Duration(5, TimeUnit.SECONDS))
      val dataFile = new org.apache.hadoop.fs.Path(file)

      val reader =
        AvroParquetReader.builder[GenericRecord](HadoopInputFile.fromPath(dataFile, conf)).withConf(conf).build()

      var r: GenericRecord = reader.read()

      val b: mutable.Builder[GenericRecord, Seq[GenericRecord]] = Seq.newBuilder[GenericRecord]

      while (r != null) {
        b += r
        r = reader.read()
      }
      b.result().length shouldEqual 3
    }

  }

}
