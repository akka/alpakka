/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.jms.javadsl

import javax.jms.Message

import akka.NotUsed
import akka.stream.alpakka.jms.{JmsSourceSettings, JmsSourceStage}

import scala.collection.JavaConversions

object JmsSource {

  /**
   * Java API: Creates an [[JmsSource]] for [[javax.jms.Message]]
   */
  def create(jmsSourceSettings: JmsSourceSettings): akka.stream.javadsl.Source[Message, NotUsed] =
    akka.stream.javadsl.Source.fromGraph(new JmsSourceStage(jmsSourceSettings))

  /**
   * Java API: Creates an [[JmsSource]] for texts
   */
  def textSource(jmsSourceSettings: JmsSourceSettings): akka.stream.javadsl.Source[String, NotUsed] =
    akka.stream.alpakka.jms.scaladsl.JmsSource.textSource(jmsSourceSettings).asJava

  /**
   * Java API: Creates an [[JmsSource]] for byte arrays
   */
  def bytesSource(jmsSourceSettings: JmsSourceSettings): akka.stream.javadsl.Source[Array[Byte], NotUsed] =
    akka.stream.alpakka.jms.scaladsl.JmsSource.bytesSource(jmsSourceSettings).asJava

  /**
   * Java API: Creates an [[JmsSource]] for Maps with primitive data types
   */
  def mapSource(
      jmsSourceSettings: JmsSourceSettings
  ): akka.stream.javadsl.Source[java.util.Map[String, Any], NotUsed] =
    akka.stream.alpakka.jms.scaladsl.JmsSource
      .mapSource(jmsSourceSettings)
      .map(scalaMap => JavaConversions.mapAsJavaMap(scalaMap))
      .asJava

  /**
   * Java API: Creates an [[JmsSource]] for serializable objects
   */
  def objectSource(jmsSourceSettings: JmsSourceSettings): akka.stream.javadsl.Source[java.io.Serializable, NotUsed] =
    akka.stream.alpakka.jms.scaladsl.JmsSource.objectSource(jmsSourceSettings).asJava
}
