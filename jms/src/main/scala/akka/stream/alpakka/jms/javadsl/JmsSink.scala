/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.jms.javadsl

import akka.NotUsed
import akka.stream.alpakka.jms.{JmsSinkSettings, JmsSinkStage}

object JmsSink {

  /**
   * Java API: Creates an [[JmsSink]]
   */
  def create(jmsSinkSettings: JmsSinkSettings): akka.stream.javadsl.Sink[String, NotUsed] =
    akka.stream.javadsl.Sink.fromGraph(new JmsSinkStage(jmsSinkSettings))

}
