/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.jms.javadsl

import javax.jms.Message
import akka.NotUsed
import akka.stream.alpakka.jms._
import akka.stream.javadsl.Source

import scala.jdk.CollectionConverters._

/**
 * Factory methods to create JMS consumers.
 */
object JmsConsumer {

  /**
   * Creates a source emitting [[javax.jms.Message]] instances, and materializes a
   * control instance to shut down the consumer.
   */
  def create(settings: JmsConsumerSettings): akka.stream.javadsl.Source[Message, JmsConsumerControl] =
    akka.stream.alpakka.jms.scaladsl.JmsConsumer.apply(settings).mapMaterializedValue(toConsumerControl).asJava

  /**
   * Creates a source emitting Strings, and materializes a
   * control instance to shut down the consumer.
   */
  def textSource(settings: JmsConsumerSettings): akka.stream.javadsl.Source[String, JmsConsumerControl] =
    akka.stream.alpakka.jms.scaladsl.JmsConsumer.textSource(settings).mapMaterializedValue(toConsumerControl).asJava

  /**
   * Creates a source emitting byte arrays, and materializes a
   * control instance to shut down the consumer.
   */
  def bytesSource(settings: JmsConsumerSettings): akka.stream.javadsl.Source[Array[Byte], JmsConsumerControl] =
    akka.stream.alpakka.jms.scaladsl.JmsConsumer.bytesSource(settings).mapMaterializedValue(toConsumerControl).asJava

  /**
   * Creates a source emitting maps, and materializes a
   * control instance to shut down the consumer.
   */
  def mapSource(
      settings: JmsConsumerSettings
  ): akka.stream.javadsl.Source[java.util.Map[String, Any], JmsConsumerControl] =
    akka.stream.alpakka.jms.scaladsl.JmsConsumer
      .mapSource(settings)
      .map(_.asJava)
      .mapMaterializedValue(toConsumerControl)
      .asJava

  /**
   * Creates a source emitting de-serialized objects, and materializes a
   * control instance to shut down the consumer.
   */
  def objectSource(
      settings: JmsConsumerSettings
  ): akka.stream.javadsl.Source[java.io.Serializable, JmsConsumerControl] =
    akka.stream.alpakka.jms.scaladsl.JmsConsumer.objectSource(settings).mapMaterializedValue(toConsumerControl).asJava

  /**
   * Creates a source emitting [[akka.stream.alpakka.jms.AckEnvelope AckEnvelope]] instances, and materializes a
   * control instance to shut down the consumer.
   * It requires explicit acknowledgements on the envelopes. The acknowledgements must be called on the envelope and not on the message inside.
   */
  def ackSource(settings: JmsConsumerSettings): akka.stream.javadsl.Source[AckEnvelope, JmsConsumerControl] =
    akka.stream.alpakka.jms.scaladsl.JmsConsumer.ackSource(settings).mapMaterializedValue(toConsumerControl).asJava

  /**
   * Creates a source emitting [[akka.stream.alpakka.jms.TxEnvelope TxEnvelope]] instances, and materializes a
   * control instance to shut down the consumer.
   * It requires explicit committing or rollback on the envelopes.
   */
  def txSource(settings: JmsConsumerSettings): akka.stream.javadsl.Source[TxEnvelope, JmsConsumerControl] =
    akka.stream.alpakka.jms.scaladsl.JmsConsumer.txSource(settings).mapMaterializedValue(toConsumerControl).asJava

  /**
   * Creates a source browsing a JMS destination (which does not consume the messages)
   * and emitting [[javax.jms.Message]] instances.
   */
  def browse(settings: JmsBrowseSettings): akka.stream.javadsl.Source[Message, NotUsed] =
    akka.stream.alpakka.jms.scaladsl.JmsConsumer.browse(settings).asJava

  private def toConsumerControl(scalaControl: scaladsl.JmsConsumerControl) = new JmsConsumerControl {

    override def connectorState(): Source[JmsConnectorState, NotUsed] =
      scalaControl.connectorState.map(_.asJava).asJava

    override def shutdown(): Unit = scalaControl.shutdown()

    override def abort(ex: Throwable): Unit = scalaControl.abort(ex)
  }
}
