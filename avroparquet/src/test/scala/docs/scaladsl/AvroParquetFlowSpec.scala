/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package docs.scaladsl

import java.util.concurrent.TimeUnit

import akka.stream.alpakka.avroparquet.scaladsl.AvroParquetFlow
import akka.stream.scaladsl.{Flow, Sink, Source}
import akka.{Done, NotUsed}
import org.apache.avro.generic.GenericRecord
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.avro.{AvroParquetReader, AvroParquetWriter, AvroReadSupport}
import org.apache.parquet.hadoop.ParquetWriter
import org.apache.parquet.hadoop.util.HadoopInputFile
import org.specs2.mutable.Specification

import scala.collection.mutable
import scala.concurrent.Await
import scala.concurrent.duration.Duration

class AvroParquetFlowSpec extends Specification with AbstractAvroParquet {

  "AvroParquet" should {

    "insert records in parquet as part of Flow stage" in {

      val docs = List[Document](Document("id1", "sdaada"), Document("id1", "sdaada"), Document("id3", " fvrfecefedfww"))

      val source = Source.fromIterator(() => docs.iterator)

      val file = folder + "/test.parquet"

      val conf = new Configuration()
      conf.setBoolean(AvroReadSupport.AVRO_COMPATIBILITY, true)

      val writer: ParquetWriter[GenericRecord] =
        AvroParquetWriter.builder[GenericRecord](new Path(file)).withConf(conf).withSchema(schema).build()

      //#init-flow
      val flow: Flow[GenericRecord, GenericRecord, NotUsed] = AvroParquetFlow(writer)

      val result = source
        .map(f => docToRecord(f))
        .via(flow)
        .runWith(Sink.ignore)
      //#init-flow

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
