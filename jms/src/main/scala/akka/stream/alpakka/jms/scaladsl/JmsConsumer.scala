/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.jms.scaladsl

import akka.NotUsed
import akka.stream.alpakka.jms._
import akka.stream.alpakka.jms.impl._
import akka.stream.scaladsl.Source
import javax.jms

import scala.collection.JavaConverters._

/**
 * Factory methods to create JMS consumers.
 */
object JmsConsumer {

  /**
   * Creates a source emitting [[javax.jms.Message]] instances, and materializes a
   * control instance to shut down the consumer.
   */
  def apply(settings: JmsConsumerSettings): Source[javax.jms.Message, JmsConsumerControl] = settings.destination match {
    case None => throw new IllegalArgumentException(noConsumerDestination(settings))
    case Some(destination) =>
      Source.fromGraph(new JmsConsumerStage(settings, destination)).mapMaterializedValue(toConsumerControl)
  }

  /**
   * Creates a source emitting Strings, and materializes a
   * control instance to shut down the consumer.
   */
  def textSource(settings: JmsConsumerSettings): Source[String, JmsConsumerControl] =
    apply(settings).map(msg => msg.asInstanceOf[jms.TextMessage].getText)

  /**
   * Creates a source emitting maps, and materializes a
   * control instance to shut down the consumer.
   */
  def mapSource(settings: JmsConsumerSettings): Source[Map[String, Any], JmsConsumerControl] =
    apply(settings).map { msg =>
      val mapMessage = msg.asInstanceOf[jms.MapMessage]

      mapMessage.getMapNames.asScala.foldLeft(Map[String, Any]()) { (result, key) =>
        val keyAsString = key.toString
        val value = mapMessage.getObject(keyAsString)
        result.+(keyAsString -> value)
      }
    }

  /**
   * Creates a source emitting byte arrays, and materializes a
   * control instance to shut down the consumer.
   */
  def bytesSource(settings: JmsConsumerSettings): Source[Array[Byte], JmsConsumerControl] =
    apply(settings).map { msg =>
      val byteMessage = msg.asInstanceOf[jms.BytesMessage]
      val byteArray = new Array[Byte](byteMessage.getBodyLength.toInt)
      byteMessage.readBytes(byteArray)
      byteArray
    }

  /**
   * Creates a source emitting de-serialized objects, and materializes a
   * control instance to shut down the consumer.
   */
  def objectSource(settings: JmsConsumerSettings): Source[java.io.Serializable, JmsConsumerControl] =
    apply(settings).map(msg => msg.asInstanceOf[jms.ObjectMessage].getObject)

  /**
   * Creates a source emitting [[akka.stream.alpakka.jms.AckEnvelope AckEnvelope]] instances, and materializes a
   * control instance to shut down the consumer.
   * It requires explicit acknowledgements on the envelopes. The acknowledgements must be called on the envelope and not on the message inside.
   */
  def ackSource(settings: JmsConsumerSettings): Source[AckEnvelope, JmsConsumerControl] = settings.destination match {
    case None => throw new IllegalArgumentException(noConsumerDestination(settings))
    case Some(destination) =>
      Source.fromGraph(new JmsAckSourceStage(settings, destination)).mapMaterializedValue(toConsumerControl)
  }

  /**
   * Creates a source emitting [[akka.stream.alpakka.jms.TxEnvelope TxEnvelope]] instances, and materializes a
   * control instance to shut down the consumer.
   * It requires explicit committing or rollback on the envelopes.
   */
  def txSource(settings: JmsConsumerSettings): Source[TxEnvelope, JmsConsumerControl] = settings.destination match {
    case None => throw new IllegalArgumentException(noConsumerDestination(settings))
    case Some(destination) =>
      Source.fromGraph(new JmsTxSourceStage(settings, destination)).mapMaterializedValue(toConsumerControl)
  }

  /**
   * Creates a source browsing a JMS destination (which does not consume the messages)
   * and emitting [[javax.jms.Message]] instances.
   * Completes: when all messages have been read
   */
  def browse(settings: JmsBrowseSettings): Source[javax.jms.Message, NotUsed] = settings.destination match {
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

    override def connectorState: Source[JmsConnectorState, NotUsed] = transformConnectorState(internal.connected)
  }

}
