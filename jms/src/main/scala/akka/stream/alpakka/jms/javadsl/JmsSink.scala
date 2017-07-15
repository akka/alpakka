/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.jms.javadsl

import akka.NotUsed
import akka.stream.alpakka.jms.{JmsSinkSettings, JmsSinkStage, JmsTextMessage}
import akka.stream.scaladsl.{Flow, Sink}

object JmsSink {

  /**
   * Java API: Creates an [[JmsSink]]
   */
  def create(jmsSinkSettings: JmsSinkSettings): akka.stream.javadsl.Sink[JmsTextMessage, NotUsed] =
    akka.stream.javadsl.Sink.fromGraph(new JmsSinkStage(jmsSinkSettings))

  /**
   * Java API: Creates an [[JmsSink]]
   */
  def textSink(jmsSinkSettings: JmsSinkSettings): akka.stream.javadsl.Sink[String, NotUsed] =
    akka.stream.alpakka.jms.scaladsl.JmsSink.textSink(jmsSinkSettings).asJava
}
