/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.jms.scaladsl

import akka.stream.alpakka.jms.JmsProducerMessage.Envelope
import akka.stream.alpakka.jms._
import akka.stream.alpakka.jms.impl.{JmsProducerMatValue, JmsProducerStage}
import akka.stream.scaladsl.{Flow, Keep, Sink, Source}
import akka.util.ByteString
import akka.{Done, NotUsed}

import scala.concurrent.Future

/**
 * Factory methods to create JMS producers.
 */
object JmsProducer {

  /**
   * Create a flow to send [[akka.stream.alpakka.jms.JmsMessage JmsMessage]] sub-classes to
   * a JMS broker.
   */
  def flow[T <: JmsMessage](settings: JmsProducerSettings): Flow[T, T, JmsProducerStatus] = settings.destination match {
    case None => throw new IllegalArgumentException(noProducerDestination(settings))
    case Some(destination) =>
      Flow[T]
        .map(m => JmsProducerMessage.Message(m, NotUsed))
        .viaMat(Flow.fromGraph(new JmsProducerStage[T, NotUsed](settings, destination)))(Keep.right)
        .mapMaterializedValue(toProducerStatus)
        .collectType[JmsProducerMessage.Message[T, NotUsed]]
        .map(_.message)

  }

  /**
   * Create a flow to send [[akka.stream.alpakka.jms.JmsProducerMessage.Envelope JmsProducerMessage.Envelope]] sub-classes to
   * a JMS broker to support pass-through of data.
   */
  def flexiFlow[T <: JmsMessage, PassThrough](
      settings: JmsProducerSettings
  ): Flow[Envelope[T, PassThrough], Envelope[T, PassThrough], JmsProducerStatus] = settings.destination match {
    case None => throw new IllegalArgumentException(noProducerDestination(settings))
    case Some(destination) =>
      Flow
        .fromGraph(new JmsProducerStage[T, PassThrough](settings, destination))
        .mapMaterializedValue(toProducerStatus)
  }

  /**
   * Create a sink to send [[akka.stream.alpakka.jms.JmsMessage JmsMessage]] sub-classes to
   * a JMS broker.
   */
  def apply(settings: JmsProducerSettings): Sink[JmsMessage, Future[Done]] =
    flow(settings).toMat(Sink.ignore)(Keep.right)

  /**
   * Create a sink to send Strings as text messages to a JMS broker.
   */
  def textSink(settings: JmsProducerSettings): Sink[String, Future[Done]] =
    Flow.fromFunction((s: String) => JmsTextMessage(s)).via(flow(settings)).toMat(Sink.ignore)(Keep.right)

  /**
   * Create a sink to send byte arrays to a JMS broker.
   */
  def bytesSink(settings: JmsProducerSettings): Sink[Array[Byte], Future[Done]] =
    Flow.fromFunction((s: Array[Byte]) => JmsByteMessage(s)).via(flow(settings)).toMat(Sink.ignore)(Keep.right)

  /**
   * Create a sink to send [[akka.util.ByteString ByteString]]s to a JMS broker.
   */
  def byteStringSink(settings: JmsProducerSettings): Sink[ByteString, Future[Done]] =
    Flow.fromFunction((s: ByteString) => JmsByteStringMessage(s)).via(flow(settings)).toMat(Sink.ignore)(Keep.right)

  /**
   * Create a sink to send map structures to a JMS broker.
   */
  def mapSink(settings: JmsProducerSettings): Sink[Map[String, Any], Future[Done]] =
    Flow.fromFunction((s: Map[String, Any]) => JmsMapMessage(s)).via(flow(settings)).toMat(Sink.ignore)(Keep.right)

  /**
   * Create a sink to send serialized objects to a JMS broker.
   */
  def objectSink(settings: JmsProducerSettings): Sink[java.io.Serializable, Future[Done]] =
    Flow
      .fromFunction((s: java.io.Serializable) => JmsObjectMessage(s))
      .via(flow(settings))
      .toMat(Sink.ignore)(Keep.right)

  private def toProducerStatus(internal: JmsProducerMatValue) = new JmsProducerStatus {

    override def connectorState: Source[JmsConnectorState, NotUsed] = transformConnectorState(internal.connected)
  }

  private def noProducerDestination(settings: JmsProducerSettings) =
    s"""Unable to create JmsProducer: it  needs a default destination to send messages to, but none was provided in
      |$settings
      |Please use withQueue, withTopic or withDestination to specify a destination.""".stripMargin
}
