/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.avroparquet.javadsl

import java.util.concurrent.CompletionStage
import akka.stream.alpakka.avroparquet.scaladsl.AvroParquetFlow
import akka.{Done, NotUsed}
import akka.stream.javadsl.{Flow, Keep, Sink}
import org.apache.avro.generic.GenericRecord
import org.apache.parquet.hadoop.ParquetWriter

object AvroParquetSink {

  def create(writer: ParquetWriter[GenericRecord]): Sink[GenericRecord, CompletionStage[Done]] =
    Flow.fromGraph(new AvroParquetFlow(writer: ParquetWriter[GenericRecord]))
      .toMat(Sink.ignore(), Keep.right[NotUsed, CompletionStage[Done]])

}
