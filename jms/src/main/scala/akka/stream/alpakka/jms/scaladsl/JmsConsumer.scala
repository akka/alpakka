/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.jms.scaladsl

import javax.jms._
import akka.NotUsed
import akka.stream.KillSwitch
import akka.stream.alpakka.jms._
import akka.stream.scaladsl.Source

import scala.collection.JavaConverters._

object JmsConsumer {

  /**
   * Scala API: Creates an [[JmsConsumer]] for [[javax.jms.Message]] instances
   */
  def apply(settings: JmsConsumerSettings): Source[Message, KillSwitch] = {
    require(settings.destination.isDefined, noConsumerDestination(settings))
    Source.fromGraph(new JmsConsumerStage(settings, settings.destination.get))
  }

  /**
   * Scala API: Creates an [[JmsConsumer]] for texts
   */
  def textSource(settings: JmsConsumerSettings): Source[String, KillSwitch] =
    apply(settings).map(msg => msg.asInstanceOf[TextMessage].getText)

  /**
   * Scala API: Creates an [[JmsConsumer]] for Maps with primitive datatypes
   */
  def mapSource(settings: JmsConsumerSettings): Source[Map[String, Any], KillSwitch] =
    apply(settings).map { msg =>
      val mapMessage = msg.asInstanceOf[MapMessage]

      mapMessage.getMapNames.asScala.foldLeft(Map[String, Any]()) { (result, key) =>
        val keyAsString = key.toString
        val value = mapMessage.getObject(keyAsString)
        result.+(keyAsString -> value)
      }
    }

  /**
   * Scala API: Creates an [[JmsConsumer]] for byte arrays
   */
  def bytesSource(settings: JmsConsumerSettings): Source[Array[Byte], KillSwitch] =
    apply(settings).map { msg =>
      val byteMessage = msg.asInstanceOf[BytesMessage]
      val byteArray = new Array[Byte](byteMessage.getBodyLength.toInt)
      byteMessage.readBytes(byteArray)
      byteArray
    }

  /**
   * Scala API: Creates an [[JmsConsumer]] for serializable objects
   */
  def objectSource(settings: JmsConsumerSettings): Source[java.io.Serializable, KillSwitch] =
    apply(settings).map(msg => msg.asInstanceOf[ObjectMessage].getObject)

  /**
   * Scala API: Creates a [[JmsConsumer]] of envelopes containing messages. It requires explicit acknowledgements
   * on the envelopes. The acknowledgements must be called on the envelope and not on the message inside.
   *
   * @param settings The settings for the ack source.
   * @return Source for JMS messages in an AckEnvelope.
   */
  def ackSource(settings: JmsConsumerSettings): Source[AckEnvelope, KillSwitch] = {
    require(settings.destination.isDefined, noConsumerDestination(settings))
    Source.fromGraph(new JmsAckSourceStage(settings, settings.destination.get))
  }

  /**
   * Scala API: Creates a [[JmsConsumer]] of envelopes containing messages. It requires explicit
   * commit or rollback on the envelope.
   *
   * @param settings The settings for the tx source
   * @return Source of the JMS messages in a TxEnvelope
   */
  def txSource(settings: JmsConsumerSettings): Source[TxEnvelope, KillSwitch] = {
    require(settings.destination.isDefined, noConsumerDestination(settings))
    Source.fromGraph(new JmsTxSourceStage(settings, settings.destination.get))
  }

  /**
   * Scala API: Creates a [[JmsConsumer]] for browsing messages non-destructively
   */
  def browse(settings: JmsBrowseSettings): Source[Message, NotUsed] = {
    require(settings.destination.isDefined, noBrowseDestination(settings))
    Source.fromGraph(new JmsBrowseStage(settings, settings.destination.get))
  }

  private def noConsumerDestination(settings: JmsConsumerSettings) =
    s"""Unable to create JmsConsumer: its needs a destination to read messages from, but none was provided in
       |$settings
       |Please use withQueue, withTopic or withDestination to specify a destination.""".stripMargin

  private def noBrowseDestination(settings: JmsBrowseSettings) =
    s"""Unable to create JmsConsumer browser: its needs a destination to read messages from, but none was provided in
       |$settings
       |Please use withQueue or withDestination to specify a destination.""".stripMargin
}
