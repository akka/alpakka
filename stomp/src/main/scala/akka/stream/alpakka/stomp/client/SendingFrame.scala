/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.stomp.client

import java.util

import io.vertx.core.buffer.{Buffer => VertxBuffer}
import io.vertx.ext.stomp.{Frame => VertxFrame}

case class SendingFrame(headers: Map[String, String], body: Vector[Byte]) {

  import scala.collection.JavaConverters._
  def toVertexFrame: VertxFrame = {
    val obj = new VertxFrame().setCommand(VertxFrame.Command.SEND)
    val h: util.Map[String, String] = headers.asJava
    if (!h.isEmpty) {
      obj.setHeaders(h)
    }
    if (body.nonEmpty) {
      obj.setBody(VertxBuffer.buffer(body.toArray))
    }
    obj
  }

  def withDestination(destination: String) =
    copy(headers + ("destination" -> destination))
}

object SendingFrame {
  import scala.collection.JavaConverters._
  def from(frame: VertxFrame): SendingFrame = {
    val body: scala.collection.immutable.Vector[Byte] =
      if (!frame.hasEmptyBody) frame.getBodyAsByteArray.toVector
      else {
        Vector()
      }
    val headers: scala.collection.immutable.Map[String, String] = if (frame.getHeaders == null) {
      Map()
    } else frame.getHeaders.asScala.toMap
    SendingFrame(headers, body)
  }
  def from(bodyStr: String): SendingFrame =
    from(bodyStr.getBytes().toVector)
  def from(body: Vector[Byte]): SendingFrame =
    SendingFrame(Map(), body)
  def from(destination: String, body: Vector[Byte]): SendingFrame =
    from(body).withDestination(destination)
}
