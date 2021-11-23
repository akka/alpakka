/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.text.impl

import java.nio.charset.Charset

import akka.annotation.InternalApi
import akka.stream.stage.{GraphStage, GraphStageLogic}
import akka.stream.{Attributes, FlowShape, Inlet, Outlet}
import akka.util.ByteString

/**
 * Decodes a stream of bytes into a stream of characters, using a supplied [[java.nio.charset.Charset]].
 */
@InternalApi
private[text] class CharsetDecodingFlow(incoming: Charset) extends GraphStage[FlowShape[ByteString, String]] {
  final private val in = Inlet[ByteString]("in")
  final private val out = Outlet[String]("out")
  override val shape: FlowShape[ByteString, String] = FlowShape(in, out)

  def createLogic(attributes: Attributes): GraphStageLogic =
    new DecodingLogic(in, out, shape, incoming)
}
