/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.file.impl.archive

import java.util.zip.{ZipEntry, ZipOutputStream}

import akka.annotation.InternalApi
import akka.event.Logging
import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, OutHandler}
import akka.stream.{Attributes, FlowShape, Inlet, Outlet}
import akka.util.{ByteString, ByteStringBuilder}

/**
 * INTERNAL API
 */
@InternalApi private[file] final class ZipArchiveFlowStage(
    val shape: FlowShape[ByteString, ByteString],
    deflateCompression: Option[Int] = None
) extends GraphStageLogic(shape) {

  private def in = shape.in
  private def out = shape.out

  private val builder = new ByteStringBuilder()
  private val zip = new ZipOutputStream(builder.asOutputStream)
  private var emptyStream = true

  setHandler(
    out,
    new OutHandler {
      override def onPull(): Unit =
        if (isClosed(in)) {
          emptyStream = true
          completeStage()
        } else {
          pull(in)
        }
    }
  )

  setHandler(
    in,
    new InHandler {
      override def onPush(): Unit = {
        emptyStream = false
        val element = grab(in)
        element match {
          case b: ByteString if FileByteStringSeparators.isStartingByteString(b) =>
            val name = FileByteStringSeparators.getPathFromStartingByteString(b)
            zip.putNextEntry(new ZipEntry(name))
          case b: ByteString if FileByteStringSeparators.isEndingByteString(b) =>
            zip.closeEntry()
          case b: ByteString =>
            val array = b.toArray
            zip.write(array, 0, array.length)
        }
        zip.flush()
        val result = builder.result()
        if (result.nonEmpty) {
          builder.clear()
          push(out, result)
        } else {
          pull(in)
        }
      }

      override def onUpstreamFinish(): Unit = {
        if (!emptyStream) {
          zip.close()
          emit(out, builder.result())
          builder.clear()
        }
        super.onUpstreamFinish()
      }

    }
  )

  override def preStart(): Unit =
    deflateCompression.foreach(l => zip.setLevel(l))
}

/**
 * INTERNAL API
 */
@InternalApi private[file] final class ZipArchiveFlow(deflateCompression: Option[Int] = None) extends GraphStage[FlowShape[ByteString, ByteString]] {

  val in: Inlet[ByteString] = Inlet(Logging.simpleName(this) + ".in")
  val out: Outlet[ByteString] = Outlet(Logging.simpleName(this) + ".out")

  override def initialAttributes: Attributes =
    Attributes.name(Logging.simpleName(this))

  override val shape: FlowShape[ByteString, ByteString] = FlowShape(in, out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new ZipArchiveFlowStage(shape, deflateCompression)
}
