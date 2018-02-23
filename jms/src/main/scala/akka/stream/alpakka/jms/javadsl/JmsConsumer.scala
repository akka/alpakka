/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.jms.javadsl

import javax.jms.Message

import akka.NotUsed
import akka.stream.KillSwitch
import akka.stream.alpakka.jms._

import scala.collection.JavaConversions

object JmsConsumer {

  /**
   * Java API: Creates an [[JmsConsumer]] for [[javax.jms.Message]]
   */
  def create(settings: JmsConsumerSettings): akka.stream.javadsl.Source[Message, KillSwitch] =
    akka.stream.javadsl.Source.fromGraph(new JmsConsumerStage(settings))

  /**
   * Java API: Creates an [[JmsConsumer]] for texts
   */
  def textSource(settings: JmsConsumerSettings): akka.stream.javadsl.Source[String, KillSwitch] =
    akka.stream.alpakka.jms.scaladsl.JmsConsumer.textSource(settings).asJava

  /**
   * Java API: Creates an [[JmsConsumer]] for byte arrays
   */
  def bytesSource(settings: JmsConsumerSettings): akka.stream.javadsl.Source[Array[Byte], KillSwitch] =
    akka.stream.alpakka.jms.scaladsl.JmsConsumer.bytesSource(settings).asJava

  /**
   * Java API: Creates an [[JmsConsumer]] for Maps with primitive data types
   */
  def mapSource(
      settings: JmsConsumerSettings
  ): akka.stream.javadsl.Source[java.util.Map[String, Any], KillSwitch] =
    akka.stream.alpakka.jms.scaladsl.JmsConsumer
      .mapSource(settings)
      .map(scalaMap => JavaConversions.mapAsJavaMap(scalaMap))
      .asJava

  /**
   * Java API: Creates an [[JmsConsumer]] for serializable objects
   */
  def objectSource(
      settings: JmsConsumerSettings
  ): akka.stream.javadsl.Source[java.io.Serializable, KillSwitch] =
    akka.stream.alpakka.jms.scaladsl.JmsConsumer.objectSource(settings).asJava

  /**
   * Java API: Creates a [[JmsConsumer]] of envelopes containing messages. It requires explicit acknowledgements
   * on the envelopes. The acknowledgements must be called on the envelope and not on the message inside.
   *
   * @param settings The settings for the ack source.
   * @return Source for JMS messages in an AckEnvelope.
   */
  def ackSource(settings: JmsConsumerSettings): akka.stream.javadsl.Source[AckEnvelope, KillSwitch] =
    akka.stream.javadsl.Source.fromGraph(new JmsAckSourceStage(settings))

  /**
   * Java API: Creates a [[JmsConsumer]] of envelopes containing messages. It requires explicit
   * commit or rollback on the envelope.
   *
   * @param settings The settings for the tx source
   * @return Source of the JMS messages in a TxEnvelope
   */
  def txSource(settings: JmsConsumerSettings): akka.stream.javadsl.Source[TxEnvelope, KillSwitch] =
    akka.stream.javadsl.Source.fromGraph(new JmsTxSourceStage(settings))

  /**
   * Java API: Creates a [[JmsConsumer]] for browsing messages non-destructively
   */
  def browse(settings: JmsBrowseSettings): akka.stream.javadsl.Source[Message, NotUsed] =
    akka.stream.javadsl.Source.fromGraph(new JmsBrowseStage(settings))
}
