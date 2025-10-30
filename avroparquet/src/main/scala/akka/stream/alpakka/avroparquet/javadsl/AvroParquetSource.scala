/*
 * Copyright (C) since 2016 Lightbend Inc. <https://akka.io>
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
