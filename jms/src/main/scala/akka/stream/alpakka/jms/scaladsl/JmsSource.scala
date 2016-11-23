/*
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.jms.scaladsl

import javax.jms.{ ConnectionFactory, Message, TextMessage }

import akka.NotUsed
import akka.stream.alpakka.jms.{ JmsSourceSettings, JmsSourceStage }
import akka.stream.scaladsl.Source

object JmsSource {

  /**
   * Scala API: Creates an [[JmsSource]]
   * @param jmsSettings the connection settings
   * @return a [[akka.stream.scaladsl.Source of jms [[javax.jms.Message]]
   */
  def apply(jmsSettings: JmsSourceSettings): Source[Message, NotUsed] =
    Source.fromGraph(new JmsSourceStage(jmsSettings))

  /**
   * Scala API: Creates an [[JmsSource]]
   * @param jmsSettings the connection settings
   * @return a [[akka.stream.scaladsl.Source of [[String]]
   */
  def textSource(jmsSettings: JmsSourceSettings): Source[String, NotUsed] =
    Source.fromGraph(new JmsSourceStage(jmsSettings)).map(msg => msg.asInstanceOf[TextMessage].getText)

}
