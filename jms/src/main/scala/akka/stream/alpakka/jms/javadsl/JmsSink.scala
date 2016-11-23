/*
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.jms.javadsl

import akka.NotUsed
import akka.stream.alpakka.jms.{ JmsSinkSettings, JmsSinkStage }

object JmsSink {

  /**
   * Java API: Creates an [[JmsSink]]
   *
   * @param jmsSinkSettings the jms connexion settings
   * @return a [[akka.stream.javadsl.Sink]]
   */
  def create(jmsSinkSettings: JmsSinkSettings): akka.stream.javadsl.Sink[String, NotUsed] =
    akka.stream.javadsl.Sink.fromGraph(new JmsSinkStage(jmsSinkSettings))

}
