/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.jms.scaladsl

import javax.jms._

import akka.stream.KillSwitch
import akka.stream.alpakka.jms._
import akka.stream.scaladsl.Source

import scala.collection.JavaConversions._

object JmsSource {

  /**
   * Scala API: Creates an [[JmsSource]] for [[javax.jms.Message]] instances
   */
  def apply(jmsSettings: JmsSourceSettings): Source[Message, KillSwitch] =
    Source.fromGraph(new JmsSourceStage(jmsSettings))

  /**
   * Scala API: Creates an [[JmsSource]] for texts
   */
  def textSource(jmsSettings: JmsSourceSettings): Source[String, KillSwitch] =
    apply(jmsSettings).map(msg => msg.asInstanceOf[TextMessage].getText)

  /**
   * Scala API: Creates an [[JmsSource]] for Maps with primitive datatypes
   */
  def mapSource(jmsSettings: JmsSourceSettings): Source[Map[String, Any], KillSwitch] =
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
  def bytesSource(jmsSettings: JmsSourceSettings): Source[Array[Byte], KillSwitch] =
    apply(jmsSettings).map { msg =>
      val byteMessage = msg.asInstanceOf[BytesMessage]
      val byteArray = new Array[Byte](byteMessage.getBodyLength.toInt)
      byteMessage.readBytes(byteArray)
      byteArray
    }

  /**
   * Scala API: Creates an [[JmsSource]] for serializable objects
   */
  def objectSource(jmsSettings: JmsSourceSettings): Source[java.io.Serializable, KillSwitch] =
    apply(jmsSettings).map(msg => msg.asInstanceOf[ObjectMessage].getObject)

  /**
   * Scala API: Creates a [[JmsSource]] of envelopes containing messages. It requires explicit acknowledgements
   * on the envelopes. The acknowledgements must be called on the envelope and not on the message inside.
   * @param jmsSettings The settings for the ack source.
   * @return Source for JMS messages in an AckEnvelope.
   */
  def ackSource(jmsSettings: JmsSourceSettings): Source[AckEnvelope, KillSwitch] =
    Source.fromGraph(new JmsAckSourceStage(jmsSettings))

  /**
   * Scala API: Creates a [[JmsSource]] of envelopes containing messages. It requires explicit
   * commit or rollback on the envelope.
   * @param jmsSettings The settings for the tx source
   * @return Source of the JMS messages in a TxEnvelope
   */
  def txSource(jmsSettings: JmsSourceSettings): Source[TxEnvelope, KillSwitch] =
    Source.fromGraph(new JmsTxSourceStage(jmsSettings))

}
