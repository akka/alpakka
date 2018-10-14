/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.jms.scaladsl

import akka.{Done, NotUsed}
import akka.stream.alpakka.jms._
import akka.stream.scaladsl.{Flow, Keep, Sink}
import akka.stream.alpakka.jms.JmsProducerMessage.Envelope

import scala.concurrent.Future

object JmsProducer {

  /**
   * Scala API: Creates an [[JmsProducer]] for [[JmsMessage]]s
   */
  def flow[T <: JmsMessage](settings: JmsProducerSettings): Flow[T, T, NotUsed] = settings.destination match {
    case None => throw new IllegalArgumentException(noProducerDestination(settings))
    case Some(destination) =>
      Flow[T]
        .map(m => JmsProducerMessage.Message(m, NotUsed))
        .via(Flow.fromGraph(new JmsProducerStage(settings, destination)))
        .collectType[JmsProducerMessage.Message[T, NotUsed]]
        .map(_.message)
  }

  /**
   * Scala API: Creates an [[JmsProducer]] for [[Envelope]]s
   */
  def flexiFlow[T <: JmsMessage, PassThrough](
      settings: JmsProducerSettings
  ): Flow[Envelope[T, PassThrough], Envelope[T, PassThrough], NotUsed] = settings.destination match {
    case None => throw new IllegalArgumentException(noProducerDestination(settings))
    case Some(destination) => Flow.fromGraph(new JmsProducerStage(settings, destination))
  }

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

  private def noProducerDestination(settings: JmsProducerSettings) =
    s"""Unable to create JmsProducer: it  needs a default destination to send messages to, but none was provided in
      |$settings
      |Please use withQueue, withTopic or withDestination to specify a destination.""".stripMargin
}
