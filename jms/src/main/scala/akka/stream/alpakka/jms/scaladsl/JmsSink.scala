/*
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.jms.scaladsl

import akka.NotUsed
import akka.stream.alpakka.jms.{JmsSinkSettings, JmsSinkStage, JmsTextMessage}
import akka.stream.scaladsl.{Flow, Keep, Sink}

object JmsSink {

  /**
   * Scala API: Creates an [[JmsSink]]
   */
  def apply(jmsSettings: JmsSinkSettings): Sink[String, NotUsed] = {
    val msgSink: Sink[JmsTextMessage, NotUsed] = Sink.fromGraph(new JmsSinkStage(jmsSettings))
    val mappingFlow: Flow[String, JmsTextMessage, NotUsed] = Flow[String].map { m: String =>
      JmsTextMessage(m)
    }
    mappingFlow.to(msgSink)
  }
}

object JmsTextMessageSink {

  /**
   * Scala API: Creates an [[JmsTextMessage]]
   */
  def apply(jmsSettings: JmsSinkSettings): Sink[JmsTextMessage, NotUsed] =
    Sink.fromGraph(new JmsSinkStage(jmsSettings))
}
