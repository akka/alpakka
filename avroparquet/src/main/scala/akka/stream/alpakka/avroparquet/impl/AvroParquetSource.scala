/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.avroparquet.impl
import akka.annotation.InternalApi
import akka.stream.{ActorAttributes, Attributes, Outlet, SourceShape}
import akka.stream.stage.{GraphStage, GraphStageLogic, OutHandler}
import org.apache.avro.generic.GenericRecord
import org.apache.parquet.hadoop.ParquetReader

/**
 * Internal API
 */
@InternalApi
private[avroparquet] class AvroParquetSource(reader: ParquetReader[GenericRecord])
    extends GraphStage[SourceShape[GenericRecord]] {

  val out: Outlet[GenericRecord] = Outlet("AvroParquetSource")

  override protected def initialAttributes: Attributes =
    super.initialAttributes and ActorAttributes.IODispatcher

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
            complete(out)
          }(push(out, _))
        }
      }
    )

    override def postStop(): Unit = reader.close()

  }
  override def shape: SourceShape[GenericRecord] = SourceShape.of(out)
}
