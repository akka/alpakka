/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.jms.scaladsl

import akka.Done
import akka.stream.alpakka.jms._
import akka.stream.scaladsl.{Flow, Keep, Sink}

import scala.concurrent.Future

object JmsSink {

  /**
   * Scala API: Creates an [[JmsSink]] for [[JmsMessage]]s
   */
  def apply(jmsSettings: JmsSinkSettings): Sink[JmsMessage, Future[Done]] =
    Sink.fromGraph(new JmsSinkStage(jmsSettings))

  /**
   * Scala API: Creates an [[JmsSink]] for strings
   */
  def textSink(jmsSettings: JmsSinkSettings): Sink[String, Future[Done]] = {
    val jmsMsgSink = Sink.fromGraph(new JmsSinkStage(jmsSettings))
    Flow.fromFunction((s: String) => JmsTextMessage(s)).toMat(jmsMsgSink)(Keep.right)
  }

  /**
   * Scala API: Creates an [[JmsSink]] for bytes
   */
  def bytesSink(jmsSettings: JmsSinkSettings): Sink[Array[Byte], Future[Done]] = {
    val jmsMsgSink = Sink.fromGraph(new JmsSinkStage(jmsSettings))
    Flow.fromFunction((s: Array[Byte]) => JmsByteMessage(s)).toMat(jmsMsgSink)(Keep.right)
  }

  /**
   * Scala API: Creates an [[JmsSink]] for maps with primitive data types
   */
  def mapSink(jmsSettings: JmsSinkSettings): Sink[Map[String, Any], Future[Done]] = {
    val jmsMsgSink = Sink.fromGraph(new JmsSinkStage(jmsSettings))
    Flow.fromFunction((s: Map[String, Any]) => JmsMapMessage(s)).toMat(jmsMsgSink)(Keep.right)
  }

  /**
   * Scala API: Creates an [[JmsSink]] for serializable objects
   */
  def objectSink(jmsSettings: JmsSinkSettings): Sink[java.io.Serializable, Future[Done]] = {
    val jmsMsgSink = Sink.fromGraph(new JmsSinkStage(jmsSettings))
    Flow.fromFunction((s: java.io.Serializable) => JmsObjectMessage(s)).toMat(jmsMsgSink)(Keep.right)
  }

}
