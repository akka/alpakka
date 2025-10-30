/*
 * Copyright (C) since 2016 Lightbend Inc. <https://akka.io>
 */

package akka.stream.alpakka.avroparquet.scaladsl
import akka.Done
import akka.stream.scaladsl.{Flow, Keep, Sink}
import org.apache.avro.generic.GenericRecord
import org.apache.parquet.hadoop.ParquetWriter

import scala.concurrent.Future

object AvroParquetSink {

  def apply[T <: GenericRecord](writer: ParquetWriter[T]): Sink[T, Future[Done]] =
    Flow.fromGraph(new akka.stream.alpakka.avroparquet.impl.AvroParquetFlow(writer)).toMat(Sink.ignore)(Keep.right)

}
