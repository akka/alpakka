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
  def flow[T <: JmsMessage](settings: JmsProducerSettings): Flow[T, T, NotUsed] =
    Flow.fromGraph(new JmsProducerStage(settings))

  /**
   * Scala API: Creates an [[JmsProducer]] for [[JmsMessage]]s
   */
  def apply(settings: JmsProducerSettings): Sink[JmsMessage, Future[Done]] =
    flow(settings).toMat(Sink.ignore)(Keep.right)

  /**
   * Scala API: Creates an [[JmsProducer]] for strings
   */
  def textSink(settings: JmsProducerSettings): Sink[String, Future[Done]] =
    Flow.fromFunction((s: String) => JmsTextMessage(s)).toMat(apply(settings))(Keep.right)

  /**
   * Scala API: Creates an [[JmsProducer]] for bytes
   */
  def bytesSink(settings: JmsProducerSettings): Sink[Array[Byte], Future[Done]] =
    Flow.fromFunction((s: Array[Byte]) => JmsByteMessage(s)).toMat(apply(settings))(Keep.right)

  /**
   * Scala API: Creates an [[JmsProducer]] for maps with primitive data types
   */
  def mapSink(settings: JmsProducerSettings): Sink[Map[String, Any], Future[Done]] =
    Flow.fromFunction((s: Map[String, Any]) => JmsMapMessage(s)).toMat(apply(settings))(Keep.right)

  /**
   * Scala API: Creates an [[JmsProducer]] for serializable objects
   */
  def objectSink(settings: JmsProducerSettings): Sink[java.io.Serializable, Future[Done]] =
    Flow.fromFunction((s: java.io.Serializable) => JmsObjectMessage(s)).toMat(apply(settings))(Keep.right)

}
