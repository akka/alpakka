/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.jms.javadsl

import akka.NotUsed
import akka.stream.KillSwitch
import akka.stream.alpakka.jms._
import javax.jms.Message

@deprecated("Use JmsConsumer instead", "0.18")
object JmsSource {

  /**
   * Java API: Creates an [[JmsSource]] for [[javax.jms.Message]]
   */
  def create(jmsSourceSettings: JmsConsumerSettings): akka.stream.javadsl.Source[Message, KillSwitch] =
    JmsConsumer.create(jmsSourceSettings)

  /**
   * Java API: Creates an [[JmsSource]] for texts
   */
  def textSource(jmsSourceSettings: JmsConsumerSettings): akka.stream.javadsl.Source[String, KillSwitch] =
    JmsConsumer.textSource(jmsSourceSettings)

  /**
   * Java API: Creates an [[JmsSource]] for byte arrays
   */
  def bytesSource(jmsSourceSettings: JmsConsumerSettings): akka.stream.javadsl.Source[Array[Byte], KillSwitch] =
    JmsConsumer.bytesSource(jmsSourceSettings)

  /**
   * Java API: Creates an [[JmsSource]] for Maps with primitive data types
   */
  def mapSource(
      jmsSourceSettings: JmsConsumerSettings
  ): akka.stream.javadsl.Source[java.util.Map[String, Any], KillSwitch] =
    JmsConsumer.mapSource(jmsSourceSettings)

  /**
   * Java API: Creates an [[JmsSource]] for serializable objects
   */
  def objectSource(
      jmsSourceSettings: JmsConsumerSettings
  ): akka.stream.javadsl.Source[java.io.Serializable, KillSwitch] =
    JmsConsumer.objectSource(jmsSourceSettings)

  /**
   * Java API: Creates a [[JmsSource]] of envelopes containing messages. It requires explicit acknowledgements
   * on the envelopes. The acknowledgements must be called on the envelope and not on the message inside.
   * @param jmsSettings The settings for the ack source.
   * @return Source for JMS messages in an AckEnvelope.
   */
  def ackSource(jmsSettings: JmsConsumerSettings): akka.stream.javadsl.Source[AckEnvelope, KillSwitch] =
    JmsConsumer.ackSource(jmsSettings)

  /**
   * Java API: Creates a [[JmsSource]] of envelopes containing messages. It requires explicit
   * commit or rollback on the envelope.
   * @param jmsSettings The settings for the tx source
   * @return Source of the JMS messages in a TxEnvelope
   */
  def txSource(jmsSettings: JmsConsumerSettings): akka.stream.javadsl.Source[TxEnvelope, KillSwitch] =
    JmsConsumer.txSource(jmsSettings)

  /**
   * Java API: Creates a [[JmsSource]] for browsing messages non-destructively
   */
  def browse(jmsSettings: JmsBrowseSettings): akka.stream.javadsl.Source[Message, NotUsed] =
    JmsConsumer.browse(jmsSettings)
}
