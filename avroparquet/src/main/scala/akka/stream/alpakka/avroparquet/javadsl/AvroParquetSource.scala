/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.avroparquet.javadsl

import akka.NotUsed
import akka.stream.javadsl.Source
import org.apache.avro.generic.GenericRecord
import org.apache.parquet.hadoop.ParquetReader

object AvroParquetSource {

  def create[T <: GenericRecord](reader: ParquetReader[T]): Source[T, NotUsed] =
    Source.fromGraph(new akka.stream.alpakka.avroparquet.impl.AvroParquetSource(reader))
}
