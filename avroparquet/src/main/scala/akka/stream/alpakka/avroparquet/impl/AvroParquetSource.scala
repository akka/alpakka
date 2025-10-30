/*
 * Copyright (C) since 2016 Lightbend Inc. <https://akka.io>
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
private[avroparquet] class AvroParquetSource[T <: GenericRecord](reader: ParquetReader[T])
    extends GraphStage[SourceShape[T]] {

  val out: Outlet[T] = Outlet("AvroParquetSource")

  override protected def initialAttributes: Attributes =
    super.initialAttributes and ActorAttributes.IODispatcher

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) {

    setHandler(
      out,
      new OutHandler {

        override def onDownstreamFinish(cause: Throwable): Unit = {
          super.onDownstreamFinish(cause)
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
  override def shape: SourceShape[T] = SourceShape.of(out)
}
