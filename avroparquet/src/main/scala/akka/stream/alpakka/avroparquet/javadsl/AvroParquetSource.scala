/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.avroparquet.scaladsl

import akka.NotUsed
import akka.stream.{Attributes, Outlet, SourceShape}
import akka.stream.scaladsl.Source
import akka.stream.stage.{GraphStage, GraphStageLogic, OutHandler}
import org.apache.avro.generic.GenericRecord
import org.apache.parquet.hadoop.ParquetReader

object AvroParquetSource {

  def apply(reader: ParquetReader[GenericRecord]): Source[GenericRecord, NotUsed] =
    Source.fromGraph(new AvroParquetSource(reader))
}

class AvroParquetSource(reader: ParquetReader[GenericRecord]) extends GraphStage[SourceShape[GenericRecord]] {

  val out: Outlet[GenericRecord] = Outlet("AvroParquetSource")

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) {

    setHandler(
      out,
      new OutHandler {

        override def onDownstreamFinish(): Unit = {
          super.onDownstreamFinish()
          reader.close()
        }

        override def onPull(): Unit = {
          val record = reader.read()
          Option(record).fold {
            reader.close()
            complete(out)
          }(push(out, _))
        }
      }
    )
  }
  override def shape: SourceShape[GenericRecord] = SourceShape.of(out)
}
