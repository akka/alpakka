/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.jms.scaladsl

import akka.Done
import akka.stream.alpakka.jms._
import akka.stream.scaladsl.Sink

import scala.concurrent.Future

@deprecated("Use JmsProducer instead", "0.18")
object JmsSink {

  /**
   * Scala API: Creates an [[JmsSink]] for [[JmsMessage]]s
   */
  def apply(jmsSettings: JmsProducerSettings): Sink[JmsMessage, Future[Done]] =
    JmsProducer.apply(jmsSettings)

  /**
   * Scala API: Creates an [[JmsSink]] for strings
   */
  def textSink(jmsSettings: JmsProducerSettings): Sink[String, Future[Done]] =
    JmsProducer.textSink(jmsSettings)

  /**
   * Scala API: Creates an [[JmsSink]] for bytes
   */
  def bytesSink(jmsSettings: JmsProducerSettings): Sink[Array[Byte], Future[Done]] =
    JmsProducer.bytesSink(jmsSettings)

  /**
   * Scala API: Creates an [[JmsSink]] for maps with primitive data types
   */
  def mapSink(jmsSettings: JmsProducerSettings): Sink[Map[String, Any], Future[Done]] =
    JmsProducer.mapSink(jmsSettings)

  /**
   * Scala API: Creates an [[JmsSink]] for serializable objects
   */
  def objectSink(jmsSettings: JmsProducerSettings): Sink[java.io.Serializable, Future[Done]] =
    JmsProducer.objectSink(jmsSettings)

}
