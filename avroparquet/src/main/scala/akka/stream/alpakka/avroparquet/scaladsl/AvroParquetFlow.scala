/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.avroparquet.scaladsl
import akka.NotUsed
import akka.stream.scaladsl.Flow
import org.apache.avro.generic.GenericRecord
import org.apache.parquet.hadoop.ParquetWriter

object AvroParquetFlow {

  def apply(writer: ParquetWriter[GenericRecord]): Flow[GenericRecord, GenericRecord, NotUsed] =
    Flow.fromGraph(new akka.stream.alpakka.avroparquet.impl.AvroParquetFlow(writer))
}
