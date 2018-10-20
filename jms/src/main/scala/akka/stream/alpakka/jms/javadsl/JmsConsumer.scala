/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.jms.javadsl

import javax.jms.Message
import akka.NotUsed
import akka.stream.alpakka.jms._
import akka.stream.javadsl.Source

import scala.collection.JavaConverters._

object JmsConsumer {

  /**
   * Java API: Creates an [[JmsConsumer]] for [[javax.jms.Message]]
   */
  def create(settings: JmsConsumerSettings): akka.stream.javadsl.Source[Message, JmsConsumerControl] =
    akka.stream.alpakka.jms.scaladsl.JmsConsumer.apply(settings).mapMaterializedValue(toConsumerControl).asJava

  /**
   * Java API: Creates an [[JmsConsumer]] for texts
   */
  def textSource(settings: JmsConsumerSettings): akka.stream.javadsl.Source[String, JmsConsumerControl] =
    akka.stream.alpakka.jms.scaladsl.JmsConsumer.textSource(settings).mapMaterializedValue(toConsumerControl).asJava

  /**
   * Java API: Creates an [[JmsConsumer]] for byte arrays
   */
  def bytesSource(settings: JmsConsumerSettings): akka.stream.javadsl.Source[Array[Byte], JmsConsumerControl] =
    akka.stream.alpakka.jms.scaladsl.JmsConsumer.bytesSource(settings).mapMaterializedValue(toConsumerControl).asJava

  /**
   * Java API: Creates an [[JmsConsumer]] for Maps with primitive data types
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
   * Java API: Creates an [[JmsConsumer]] for serializable objects
   */
  def objectSource(
      settings: JmsConsumerSettings
  ): akka.stream.javadsl.Source[java.io.Serializable, JmsConsumerControl] =
    akka.stream.alpakka.jms.scaladsl.JmsConsumer.objectSource(settings).mapMaterializedValue(toConsumerControl).asJava

  /**
   * Java API: Creates a [[JmsConsumer]] of envelopes containing messages. It requires explicit acknowledgements
   * on the envelopes. The acknowledgements must be called on the envelope and not on the message inside.
   *
   * @param settings The settings for the ack source.
   * @return Source for JMS messages in an AckEnvelope.
   */
  def ackSource(settings: JmsConsumerSettings): akka.stream.javadsl.Source[AckEnvelope, JmsConsumerControl] =
    akka.stream.alpakka.jms.scaladsl.JmsConsumer.ackSource(settings).mapMaterializedValue(toConsumerControl).asJava

  /**
   * Java API: Creates a [[JmsConsumer]] of envelopes containing messages. It requires explicit
   * commit or rollback on the envelope.
   *
   * @param settings The settings for the tx source
   * @return Source of the JMS messages in a TxEnvelope
   */
  def txSource(settings: JmsConsumerSettings): akka.stream.javadsl.Source[TxEnvelope, JmsConsumerControl] =
    akka.stream.alpakka.jms.scaladsl.JmsConsumer.txSource(settings).mapMaterializedValue(toConsumerControl).asJava

  /**
   * Java API: Creates a [[JmsConsumer]] for browsing messages non-destructively
   */
  def browse(settings: JmsBrowseSettings): akka.stream.javadsl.Source[Message, NotUsed] =
    akka.stream.alpakka.jms.scaladsl.JmsConsumer.browse(settings).asJava

  private def toConsumerControl(scalaControl: scaladsl.JmsConsumerControl) = new JmsConsumerControl {

    override def connectorState(): Source[JmsConnectorState, NotUsed] =
      transformConnectorState(scalaControl.connectorState)

    override def shutdown(): Unit = scalaControl.shutdown()

    override def abort(ex: Throwable): Unit = scalaControl.abort(ex)
  }
}
