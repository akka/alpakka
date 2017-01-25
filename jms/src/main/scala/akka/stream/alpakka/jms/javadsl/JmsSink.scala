/*
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.jms.javadsl

import akka.NotUsed
import akka.stream.alpakka.jms.{JmsMessage, JmsSinkSettings, JmsSinkStage}
import akka.stream.scaladsl.{Flow, Sink}

object JmsTextSink {

  /**
   * Java API: Creates an [[JmsTextSink]]
   */
  def create(jmsSinkSettings: JmsSinkSettings): akka.stream.javadsl.Sink[String, NotUsed] = {
//    val msgSink: akka.stream.javadsl.Sink[JmsMessage, NotUsed] =
//      akka.stream.javadsl.Sink.fromGraph(new JmsSinkStage(jmsSinkSettings))
//    val conversionFunction: akka.japi.function.Function[String, JmsMessage] = (m: String) => JmsMessage(m)
//    val mappingFlow: akka.stream.javadsl.Flow[String, JmsMessage, NotUsed] =
//      akka.stream.javadsl.Flow.fromFunction[String, JmsMessage](conversionFunction)
//    mappingFlow.to(msgSink)
    val msgSink: Sink[JmsMessage, NotUsed] = Sink.fromGraph(new JmsSinkStage(jmsSinkSettings))
    val mappingFlow: Flow[String, JmsMessage, NotUsed] = Flow[String].map { m: String =>
      JmsMessage(m)
    }
    mappingFlow.to(msgSink).asJava
  }
}

object JmsMessageSink {

  /**
   * Java API: Creates an [[JmsMessageSink]]
   */
  def create(jmsSinkSettings: JmsSinkSettings): akka.stream.javadsl.Sink[JmsMessage, NotUsed] =
    akka.stream.javadsl.Sink.fromGraph(new JmsSinkStage(jmsSinkSettings))

}
