/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.jms.javadsl

import javax.jms.Message

import akka.stream.KillSwitch
import akka.stream.alpakka.jms._

import scala.collection.JavaConversions

object JmsSource {

  /**
   * Java API: Creates an [[JmsSource]] for [[javax.jms.Message]]
   */
  def create(jmsSourceSettings: JmsSourceSettings): akka.stream.javadsl.Source[Message, KillSwitch] =
    akka.stream.javadsl.Source.fromGraph(new JmsSourceStage(jmsSourceSettings))

  /**
   * Java API: Creates an [[JmsSource]] for texts
   */
  def textSource(jmsSourceSettings: JmsSourceSettings): akka.stream.javadsl.Source[String, KillSwitch] =
    akka.stream.alpakka.jms.scaladsl.JmsSource.textSource(jmsSourceSettings).asJava

  /**
   * Java API: Creates an [[JmsSource]] for byte arrays
   */
  def bytesSource(jmsSourceSettings: JmsSourceSettings): akka.stream.javadsl.Source[Array[Byte], KillSwitch] =
    akka.stream.alpakka.jms.scaladsl.JmsSource.bytesSource(jmsSourceSettings).asJava

  /**
   * Java API: Creates an [[JmsSource]] for Maps with primitive data types
   */
  def mapSource(
      jmsSourceSettings: JmsSourceSettings
  ): akka.stream.javadsl.Source[java.util.Map[String, Any], KillSwitch] =
    akka.stream.alpakka.jms.scaladsl.JmsSource
      .mapSource(jmsSourceSettings)
      .map(scalaMap => JavaConversions.mapAsJavaMap(scalaMap))
      .asJava

  /**
   * Java API: Creates an [[JmsSource]] for serializable objects
   */
  def objectSource(
      jmsSourceSettings: JmsSourceSettings
  ): akka.stream.javadsl.Source[java.io.Serializable, KillSwitch] =
    akka.stream.alpakka.jms.scaladsl.JmsSource.objectSource(jmsSourceSettings).asJava

  /**
   * Java API: Creates a [[JmsSource]] of envelopes containing messages. It requires explicit acknowledgements
   * on the envelopes. The acknowledgements must be called on the envelope and not on the message inside.
   * @param jmsSettings The settings for the ack source.
   * @return Source for JMS messages in an AckEnvelope.
   */
  def ackSource(jmsSettings: JmsSourceSettings): akka.stream.javadsl.Source[AckEnvelope, KillSwitch] =
    akka.stream.javadsl.Source.fromGraph(new JmsAckSourceStage(jmsSettings))

  /**
   * Java API: Creates a [[JmsSource]] of envelopes containing messages. It requires explicit
   * commit or rollback on the envelope.
   * @param jmsSettings The settings for the tx source
   * @return Source of the JMS messages in a TxEnvelope
   */
  def txSource(jmsSettings: JmsSourceSettings): akka.stream.javadsl.Source[TxEnvelope, KillSwitch] =
    akka.stream.javadsl.Source.fromGraph(new JmsTxSourceStage(jmsSettings))
}
