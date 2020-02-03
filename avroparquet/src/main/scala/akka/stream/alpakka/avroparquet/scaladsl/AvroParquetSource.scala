/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.avroparquet.scaladsl
import akka.NotUsed
import akka.stream.scaladsl.Source
import org.apache.avro.generic.GenericRecord
import org.apache.parquet.hadoop.ParquetReader

object AvroParquetSource {

  def apply(reader: ParquetReader[GenericRecord]): Source[GenericRecord, NotUsed] =
    Source.fromGraph(new akka.stream.alpakka.avroparquet.impl.AvroParquetSource(reader))

}
