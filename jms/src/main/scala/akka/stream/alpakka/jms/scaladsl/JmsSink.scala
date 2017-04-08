/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.jms.scaladsl

import akka.NotUsed
import akka.stream.alpakka.jms.{JmsSinkSettings, JmsSinkStage}
import akka.stream.scaladsl.Sink

object JmsSink {

  /**
   * Scala API: Creates an [[JmsSink]]
   */
  def apply(jmsSettings: JmsSinkSettings): Sink[String, NotUsed] =
    Sink.fromGraph(new JmsSinkStage(jmsSettings))

}
