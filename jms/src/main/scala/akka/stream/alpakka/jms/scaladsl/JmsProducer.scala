/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.jms.scaladsl

import akka.{Done, NotUsed}
import akka.stream.alpakka.jms._
import akka.stream.scaladsl.{Flow, Keep, Sink}

import scala.concurrent.Future

object JmsProducer {

  /**
   * Scala API: Creates an [[JmsProducer]] for [[JmsMessage]]s
   */
  def flow(jmsSettings: JmsProducerSettings): Flow[JmsMessage, JmsMessage, NotUsed] =
    Flow.fromGraph(new JmsProducerStage(jmsSettings))

  /**
   * Scala API: Creates an [[JmsProducer]] for [[JmsMessage]]s
   */
  def apply(jmsSettings: JmsProducerSettings): Sink[JmsMessage, Future[Done]] =
    flow(jmsSettings).toMat(Sink.ignore)(Keep.right)

  /**
   * Scala API: Creates an [[JmsProducer]] for strings
   */
  def textSink(jmsSettings: JmsProducerSettings): Sink[String, Future[Done]] =
    Flow.fromFunction((s: String) => JmsTextMessage(s)).toMat(apply(jmsSettings))(Keep.right)

  /**
   * Scala API: Creates an [[JmsProducer]] for bytes
   */
  def bytesSink(jmsSettings: JmsProducerSettings): Sink[Array[Byte], Future[Done]] =
    Flow.fromFunction((s: Array[Byte]) => JmsByteMessage(s)).toMat(apply(jmsSettings))(Keep.right)

  /**
   * Scala API: Creates an [[JmsProducer]] for maps with primitive data types
   */
  def mapSink(jmsSettings: JmsProducerSettings): Sink[Map[String, Any], Future[Done]] =
    Flow.fromFunction((s: Map[String, Any]) => JmsMapMessage(s)).toMat(apply(jmsSettings))(Keep.right)

  /**
   * Scala API: Creates an [[JmsProducer]] for serializable objects
   */
  def objectSink(jmsSettings: JmsProducerSettings): Sink[java.io.Serializable, Future[Done]] =
    Flow.fromFunction((s: java.io.Serializable) => JmsObjectMessage(s)).toMat(apply(jmsSettings))(Keep.right)

}
