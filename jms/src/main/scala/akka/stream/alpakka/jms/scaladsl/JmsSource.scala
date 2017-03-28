/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.jms.scaladsl

import javax.jms.{Message, TextMessage}

import akka.NotUsed
import akka.stream.alpakka.jms.{JmsSourceSettings, JmsSourceStage}
import akka.stream.scaladsl.Source

object JmsSource {

  /**
   * Scala API: Creates an [[JmsSource]]
   */
  def apply(jmsSettings: JmsSourceSettings): Source[Message, NotUsed] =
    Source.fromGraph(new JmsSourceStage(jmsSettings))

  /**
   * Scala API: Creates an [[JmsSource]]
   */
  def textSource(jmsSettings: JmsSourceSettings): Source[String, NotUsed] =
    Source.fromGraph(new JmsSourceStage(jmsSettings)).map(msg => msg.asInstanceOf[TextMessage].getText)

}
