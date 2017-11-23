/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.jms.scaladsl

import akka.NotUsed
import akka.stream.alpakka.jms._
import akka.stream.scaladsl.{Flow, Sink}

object JmsSink {

  /**
   * Scala API: Creates an [[JmsSink]] for [[JmsMessage]]s
   */
  def apply(jmsSettings: JmsSinkSettings): Sink[JmsMessage, NotUsed] =
    Sink.fromGraph(new JmsSinkStage(jmsSettings))

  /**
   * Scala API: Creates an [[JmsSink]] for strings
   */
  def textSink(jmsSettings: JmsSinkSettings): Sink[String, NotUsed] = {
    val jmsMsgSink = Sink.fromGraph(new JmsSinkStage(jmsSettings))
    Flow.fromFunction((s: String) => JmsTextMessage(s)).to(jmsMsgSink)
  }

  /**
   * Scala API: Creates an [[JmsSink]] for bytes
   */
  def bytesSink(jmsSettings: JmsSinkSettings): Sink[Array[Byte], NotUsed] = {
    val jmsMsgSink = Sink.fromGraph(new JmsSinkStage(jmsSettings))
    Flow.fromFunction((s: Array[Byte]) => JmsByteMessage(s)).to(jmsMsgSink)
  }

  /**
   * Scala API: Creates an [[JmsSink]] for maps with primitive data types
   */
  def mapSink(jmsSettings: JmsSinkSettings): Sink[Map[String, Any], NotUsed] = {
    val jmsMsgSink = Sink.fromGraph(new JmsSinkStage(jmsSettings))
    Flow.fromFunction((s: Map[String, Any]) => JmsMapMessage(s)).to(jmsMsgSink)
  }

  /**
   * Scala API: Creates an [[JmsSink]] for serializable objects
   */
  def objectSink(jmsSettings: JmsSinkSettings): Sink[java.io.Serializable, NotUsed] = {
    val jmsMsgSink = Sink.fromGraph(new JmsSinkStage(jmsSettings))
    Flow.fromFunction((s: java.io.Serializable) => JmsObjectMessage(s)).to(jmsMsgSink)
  }

}
