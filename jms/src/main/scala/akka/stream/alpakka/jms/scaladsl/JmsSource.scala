/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.jms.scaladsl

import akka.NotUsed
import akka.stream.KillSwitch
import akka.stream.alpakka.jms._
import akka.stream.scaladsl.Source
import javax.jms._

@deprecated("Use JmsConsumer instead", "0.18")
object JmsSource {

  /**
   * Scala API: Creates an [[JmsSource]] for [[javax.jms.Message]] instances
   */
  def apply(jmsSettings: JmsConsumerSettings): Source[Message, KillSwitch] =
    JmsConsumer.apply(jmsSettings)

  /**
   * Scala API: Creates an [[JmsSource]] for texts
   */
  def textSource(jmsSettings: JmsConsumerSettings): Source[String, KillSwitch] =
    JmsConsumer.textSource(jmsSettings)

  /**
   * Scala API: Creates an [[JmsSource]] for Maps with primitive datatypes
   */
  def mapSource(jmsSettings: JmsConsumerSettings): Source[Map[String, Any], KillSwitch] =
    JmsConsumer.mapSource(jmsSettings)

  /**
   * Scala API: Creates an [[JmsSource]] for byte arrays
   */
  def bytesSource(jmsSettings: JmsConsumerSettings): Source[Array[Byte], KillSwitch] =
    JmsConsumer.bytesSource(jmsSettings)

  /**
   * Scala API: Creates an [[JmsSource]] for serializable objects
   */
  def objectSource(jmsSettings: JmsConsumerSettings): Source[java.io.Serializable, KillSwitch] =
    JmsConsumer.objectSource(jmsSettings)

  /**
   * Scala API: Creates a [[JmsSource]] of envelopes containing messages. It requires explicit acknowledgements
   * on the envelopes. The acknowledgements must be called on the envelope and not on the message inside.
   * @param jmsSettings The settings for the ack source.
   * @return Source for JMS messages in an AckEnvelope.
   */
  def ackSource(jmsSettings: JmsConsumerSettings): Source[AckEnvelope, KillSwitch] =
    JmsConsumer.ackSource(jmsSettings)

  /**
   * Scala API: Creates a [[JmsSource]] of envelopes containing messages. It requires explicit
   * commit or rollback on the envelope.
   * @param jmsSettings The settings for the tx source
   * @return Source of the JMS messages in a TxEnvelope
   */
  def txSource(jmsSettings: JmsConsumerSettings): Source[TxEnvelope, KillSwitch] =
    JmsConsumer.txSource(jmsSettings)

  /**
   * Scala API: Creates a [[JmsSource]] for browsing messages non-destructively
   */
  def browse(jmsSettings: JmsBrowseSettings): Source[Message, NotUsed] =
    JmsConsumer.browse(jmsSettings)
}
