/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.jms.scaladsl

import akka.NotUsed
import akka.stream.alpakka.jms.{JmsSinkSettings, JmsSinkStage, JmsTextMessage}
import akka.stream.scaladsl.{Flow, Keep, Sink}

object JmsSink {

  /**
   * Scala API: Creates an [[JmsSink]]
   */
  def apply(jmsSettings: JmsSinkSettings): Sink[JmsTextMessage, NotUsed] =
    Sink.fromGraph(new JmsSinkStage(jmsSettings))

  /**
   * Scala API: Creates an [[JmsSink]]
   */
  def textSink(jmsSettings: JmsSinkSettings): Sink[String, NotUsed] = {
    val jmsMsgSink = Sink.fromGraph(new JmsSinkStage(jmsSettings))
    Flow.fromFunction((s: String) => JmsTextMessage(s)).to(jmsMsgSink)
  }
}
