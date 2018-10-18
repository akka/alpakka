/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.jms.scaladsl

import akka.NotUsed
import akka.stream.alpakka.jms._
import akka.stream.alpakka.jms.impl.JmsConsumerMatValue
import akka.stream.scaladsl.Source
import javax.jms._

import scala.collection.JavaConverters._

object JmsConsumer {

  /**
   * Scala API: Creates an [[JmsConsumer]] for [[javax.jms.Message]] instances
   */
  def apply(settings: JmsConsumerSettings): Source[Message, JmsConsumerControl] = settings.destination match {
    case None => throw new IllegalArgumentException(noConsumerDestination(settings))
    case Some(destination) =>
      Source.fromGraph(new JmsConsumerStage(settings, destination)).mapMaterializedValue(toConsumerControl)
  }

  /**
   * Scala API: Creates an [[JmsConsumer]] for texts
   */
  def textSource(settings: JmsConsumerSettings): Source[String, JmsConsumerControl] =
    apply(settings).map(msg => msg.asInstanceOf[TextMessage].getText)

  /**
   * Scala API: Creates an [[JmsConsumer]] for Maps with primitive datatypes
   */
  def mapSource(settings: JmsConsumerSettings): Source[Map[String, Any], JmsConsumerControl] =
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
  def bytesSource(settings: JmsConsumerSettings): Source[Array[Byte], JmsConsumerControl] =
    apply(settings).map { msg =>
      val byteMessage = msg.asInstanceOf[BytesMessage]
      val byteArray = new Array[Byte](byteMessage.getBodyLength.toInt)
      byteMessage.readBytes(byteArray)
      byteArray
    }

  /**
   * Scala API: Creates an [[JmsConsumer]] for serializable objects
   */
  def objectSource(settings: JmsConsumerSettings): Source[java.io.Serializable, JmsConsumerControl] =
    apply(settings).map(msg => msg.asInstanceOf[ObjectMessage].getObject)

  /**
   * Scala API: Creates a [[JmsConsumer]] of envelopes containing messages. It requires explicit acknowledgements
   * on the envelopes. The acknowledgements must be called on the envelope and not on the message inside.
   *
   * @param settings The settings for the ack source.
   * @return Source for JMS messages in an AckEnvelope.
   */
  def ackSource(settings: JmsConsumerSettings): Source[AckEnvelope, JmsConsumerControl] = settings.destination match {
    case None => throw new IllegalArgumentException(noConsumerDestination(settings))
    case Some(destination) =>
      Source.fromGraph(new JmsAckSourceStage(settings, destination)).mapMaterializedValue(toConsumerControl)
  }

  /**
   * Scala API: Creates a [[JmsConsumer]] of envelopes containing messages. It requires explicit
   * commit or rollback on the envelope.
   *
   * @param settings The settings for the tx source
   * @return Source of the JMS messages in a TxEnvelope
   */
  def txSource(settings: JmsConsumerSettings): Source[TxEnvelope, JmsConsumerControl] = settings.destination match {
    case None => throw new IllegalArgumentException(noConsumerDestination(settings))
    case Some(destination) =>
      Source.fromGraph(new JmsTxSourceStage(settings, destination)).mapMaterializedValue(toConsumerControl)
  }

  /**
   * Scala API: Creates a [[JmsConsumer]] for browsing messages non-destructively
   */
  def browse(settings: JmsBrowseSettings): Source[Message, NotUsed] = settings.destination match {
    case None => throw new IllegalArgumentException(noBrowseDestination(settings))
    case Some(destination) => Source.fromGraph(new JmsBrowseStage(settings, destination))
  }

  private def noConsumerDestination(settings: JmsConsumerSettings) =
    s"""Unable to create JmsConsumer: its needs a destination to read messages from, but none was provided in
       |$settings
       |Please use withQueue, withTopic or withDestination to specify a destination.""".stripMargin

  private def noBrowseDestination(settings: JmsBrowseSettings) =
    s"""Unable to create JmsConsumer browser: its needs a destination to read messages from, but none was provided in
       |$settings
       |Please use withQueue or withDestination to specify a destination.""".stripMargin

  private def toConsumerControl(internal: JmsConsumerMatValue) = new JmsConsumerControl {

    override def shutdown(): Unit = internal.shutdown()

    override def abort(ex: Throwable): Unit = internal.abort(ex)

    override def connection: Source[JmsConnectorState, NotUsed] = transformConnected(internal.connected)
  }

}
