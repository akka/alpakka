/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.avroparquet.javadsl
import akka.NotUsed
import akka.stream.javadsl.Flow
import org.apache.avro.generic.GenericRecord
import org.apache.parquet.hadoop.ParquetWriter

object AvroParquetFlow {

  def create(writer: ParquetWriter[GenericRecord]): Flow[GenericRecord, GenericRecord, NotUsed] =
    Flow.fromGraph(new akka.stream.alpakka.avroparquet.impl.AvroParquetFlow(writer))
}
