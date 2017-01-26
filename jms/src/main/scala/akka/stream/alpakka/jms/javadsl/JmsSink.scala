/*
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.jms.javadsl

import akka.NotUsed
import akka.stream.alpakka.jms.{JmsSinkSettings, JmsSinkStage, JmsTextMessage}
import akka.stream.scaladsl.{Flow, Sink}

object JmsSink {

  /**
   * Java API: Creates an [[JmsSink]]
   */
  def create(jmsSinkSettings: JmsSinkSettings): akka.stream.javadsl.Sink[String, NotUsed] = {
    val msgSink: Sink[JmsTextMessage, NotUsed] = Sink.fromGraph(new JmsSinkStage(jmsSinkSettings))
    val mappingFlow: Flow[String, JmsTextMessage, NotUsed] = Flow[String].map { m: String =>
      JmsTextMessage(m)
    }
    mappingFlow.to(msgSink).asJava
  }
}

object JmsTextMessageSink {

  /**
   * Java API: Creates an [[JmsTextMessageSink]]
   */
  def create(jmsSinkSettings: JmsSinkSettings): akka.stream.javadsl.Sink[JmsTextMessage, NotUsed] =
    akka.stream.javadsl.Sink.fromGraph(new JmsSinkStage(jmsSinkSettings))

}
