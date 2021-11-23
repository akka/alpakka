/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.text.impl

import java.nio.charset.Charset

import akka.annotation.InternalApi
import akka.stream.{Attributes, FlowShape, Inlet, Outlet}
import akka.stream.stage.{GraphStage, GraphStageLogic}
import akka.util.ByteString

/**
 * Decodes a stream of bytes into a stream of characters, using a supplied [[java.nio.charset.Charset]].
 */
@InternalApi
private[text] class CharsetTranscodingFlow(incoming: Charset, outgoing: Charset)
    extends GraphStage[FlowShape[ByteString, ByteString]] {
  final private val in = Inlet[ByteString]("in")
  final private val out = Outlet[ByteString]("out")
  override val shape: FlowShape[ByteString, ByteString] = FlowShape(in, out)

  def createLogic(attributes: Attributes): GraphStageLogic =
    new TranscodingLogic(in, out, shape, incoming, outgoing)
}
