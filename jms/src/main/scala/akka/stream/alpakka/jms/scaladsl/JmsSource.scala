/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.jms.scaladsl

import javax.jms._

import akka.NotUsed
import akka.stream.alpakka.jms.{JmsSourceSettings, JmsSourceStage}
import akka.stream.scaladsl.Source

import scala.collection.JavaConversions._

object JmsSource {

  /**
   * Scala API: Creates an [[JmsSource]] for [[javax.jms.Message]] instances
   */
  def apply(jmsSettings: JmsSourceSettings): Source[Message, NotUsed] =
    Source.fromGraph(new JmsSourceStage(jmsSettings))

  /**
   * Scala API: Creates an [[JmsSource]] for texts
   */
  def textSource(jmsSettings: JmsSourceSettings): Source[String, NotUsed] =
    apply(jmsSettings).map(msg => msg.asInstanceOf[TextMessage].getText)

  /**
   * Scala API: Creates an [[JmsSource]] for Maps with primitive datatypes
   */
  def mapSource(jmsSettings: JmsSourceSettings): Source[Map[String, Any], NotUsed] =
    apply(jmsSettings).map { msg =>
      val mapMessage = msg.asInstanceOf[MapMessage]

      mapMessage.getMapNames.foldLeft(Map[String, Any]()) { (result, key) =>
        val keyAsString = key.toString
        val value = mapMessage.getObject(keyAsString)
        result.+(keyAsString -> value)
      }
    }

  /**
   * Scala API: Creates an [[JmsSource]] for byte arrays
   */
  def bytesSource(jmsSettings: JmsSourceSettings): Source[Array[Byte], NotUsed] =
    apply(jmsSettings).map { msg =>
      val byteMessage = msg.asInstanceOf[BytesMessage]
      val byteArray = new Array[Byte](byteMessage.getBodyLength.toInt)
      byteMessage.readBytes(byteArray)
      byteArray
    }

  /**
   * Scala API: Creates an [[JmsSource]] for serializable objects
   */
  def objectSource(jmsSettings: JmsSourceSettings): Source[java.io.Serializable, NotUsed] =
    apply(jmsSettings).map(msg => msg.asInstanceOf[ObjectMessage].getObject)

}
