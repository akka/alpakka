/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.avroparquet.scaladsl
import akka.NotUsed
import akka.stream.scaladsl.Flow
import org.apache.avro.generic.GenericRecord
import org.apache.parquet.hadoop.ParquetWriter

object AvroParquetFlow {

  def apply[T <: GenericRecord](writer: ParquetWriter[T]): Flow[T, T, NotUsed] =
    Flow.fromGraph(new akka.stream.alpakka.avroparquet.impl.AvroParquetFlow(writer))
}
