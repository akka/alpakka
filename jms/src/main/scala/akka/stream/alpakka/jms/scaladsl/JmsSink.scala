/*
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.jms.scaladsl

import akka.NotUsed
import akka.stream.alpakka.jms.{JmsMessage, JmsSinkSettings, JmsSinkStage}
import akka.stream.scaladsl.{Flow, Keep, Sink}

object JmsTextSink {

  /**
   * Scala API: Creates an [[JmsTextSink]]
   */
  def apply(jmsSettings: JmsSinkSettings): Sink[String, NotUsed] = {
    val msgSink: Sink[JmsMessage, NotUsed] = Sink.fromGraph(new JmsSinkStage(jmsSettings))
    val mappingFlow: Flow[String, JmsMessage, NotUsed] = Flow[String].map { m: String =>
      JmsMessage(m)
    }
    mappingFlow.to(msgSink)
  }
}

object JmsMessageSink {

  /**
   * Scala API: Creates an [[JmsMessage]]
   */
  def apply(jmsSettings: JmsSinkSettings): Sink[JmsMessage, NotUsed] =
    Sink.fromGraph(new JmsSinkStage(jmsSettings))
}
