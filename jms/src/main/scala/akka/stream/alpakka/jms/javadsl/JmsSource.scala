/*
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.jms.javadsl

import javax.jms.{ Message, TextMessage }

import akka.NotUsed
import akka.japi.Function
import akka.stream.alpakka.jms.{ JmsSourceSettings, JmsSourceStage }

object JmsSource {

  /**
   * Java API: Creates an [[JmsSource]]
   *
   * @param jmsSourceSettings the jms connexion settings
   * @return a [[akka.stream.javadsl.Source of jms [[javax.jms.Message]]
   */
  def create(jmsSourceSettings: JmsSourceSettings): akka.stream.javadsl.Source[Message, NotUsed] =
    akka.stream.javadsl.Source.fromGraph(new JmsSourceStage(jmsSourceSettings))

  /**
   * Java API: Creates an [[JmsSource]]
   *
   * @param jmsSourceSettings the jms connexion settings
   * @return a [[akka.stream.javadsl.Source of [[String]]
   */
  def textSource(jmsSourceSettings: JmsSourceSettings): akka.stream.javadsl.Source[String, NotUsed] =
    akka.stream.alpakka.jms.scaladsl.JmsSource.textSource(jmsSourceSettings).asJava

}
