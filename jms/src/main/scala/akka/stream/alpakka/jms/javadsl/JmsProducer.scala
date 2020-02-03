/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.jms.javadsl

import java.util.concurrent.CompletionStage

import akka.stream.alpakka.jms.{scaladsl, JmsEnvelope, JmsMessage, JmsProducerSettings}
import akka.stream.javadsl.Source
import akka.stream.scaladsl.{Flow, Keep}
import akka.util.ByteString
import akka.{Done, NotUsed}

import scala.collection.JavaConverters._
import scala.compat.java8.FutureConverters

/**
 * Factory methods to create JMS producers.
 */
object JmsProducer {

  /**
   * Create a flow to send [[akka.stream.alpakka.jms.JmsMessage JmsMessage]] sub-classes to
   * a JMS broker.
   */
  def flow[R <: JmsMessage](
      settings: JmsProducerSettings
  ): akka.stream.javadsl.Flow[R, R, JmsProducerStatus] =
    akka.stream.alpakka.jms.scaladsl.JmsProducer.flow(settings).mapMaterializedValue(toProducerStatus).asJava

  /**
   * Create a flow to send [[akka.stream.alpakka.jms.JmsEnvelope JmsEnvelope]] sub-classes to
   * a JMS broker to support pass-through of data.
   */
  def flexiFlow[PassThrough](
      settings: JmsProducerSettings
  ): akka.stream.javadsl.Flow[JmsEnvelope[PassThrough], JmsEnvelope[PassThrough], JmsProducerStatus] =
    akka.stream.alpakka.jms.scaladsl.JmsProducer
      .flexiFlow[PassThrough](settings)
      .mapMaterializedValue(toProducerStatus)
      .asJava

  /**
   * Create a sink to send [[akka.stream.alpakka.jms.JmsMessage JmsMessage]] sub-classes to
   * a JMS broker.
   */
  def sink[R <: JmsMessage](
      settings: JmsProducerSettings
  ): akka.stream.javadsl.Sink[R, CompletionStage[Done]] =
    akka.stream.alpakka.jms.scaladsl.JmsProducer
      .sink(settings)
      .mapMaterializedValue(FutureConverters.toJava)
      .asJava

  /**
   * Create a sink to send Strings as text messages to a JMS broker.
   */
  def textSink(settings: JmsProducerSettings): akka.stream.javadsl.Sink[String, CompletionStage[Done]] =
    akka.stream.alpakka.jms.scaladsl.JmsProducer
      .textSink(settings)
      .mapMaterializedValue(FutureConverters.toJava)
      .asJava

  /**
   * Create a sink to send byte arrays to a JMS broker.
   */
  def bytesSink(settings: JmsProducerSettings): akka.stream.javadsl.Sink[Array[Byte], CompletionStage[Done]] =
    akka.stream.alpakka.jms.scaladsl.JmsProducer
      .bytesSink(settings)
      .mapMaterializedValue(FutureConverters.toJava)
      .asJava

  /**
   * Create a sink to send [[akka.util.ByteString ByteString]]s to a JMS broker.
   */
  def byteStringSink(settings: JmsProducerSettings): akka.stream.javadsl.Sink[ByteString, CompletionStage[Done]] =
    akka.stream.alpakka.jms.scaladsl.JmsProducer
      .byteStringSink(settings)
      .mapMaterializedValue(FutureConverters.toJava)
      .asJava

  /**
   * Create a sink to send map structures to a JMS broker.
   */
  def mapSink(
      settings: JmsProducerSettings
  ): akka.stream.javadsl.Sink[java.util.Map[String, Any], CompletionStage[Done]] = {

    val scalaSink =
      akka.stream.alpakka.jms.scaladsl.JmsProducer
        .mapSink(settings)
        .mapMaterializedValue(FutureConverters.toJava)
    val javaToScalaConversion =
      Flow.fromFunction((javaMap: java.util.Map[String, Any]) => javaMap.asScala.toMap)
    javaToScalaConversion.toMat(scalaSink)(Keep.right).asJava
  }

  /**
   * Create a sink to send serialized objects to a JMS broker.
   */
  def objectSink(
      settings: JmsProducerSettings
  ): akka.stream.javadsl.Sink[java.io.Serializable, CompletionStage[Done]] =
    akka.stream.alpakka.jms.scaladsl.JmsProducer
      .objectSink(settings)
      .mapMaterializedValue(FutureConverters.toJava)
      .asJava

  private def toProducerStatus(scalaStatus: scaladsl.JmsProducerStatus) = new JmsProducerStatus {

    override def connectorState: Source[JmsConnectorState, NotUsed] =
      scalaStatus.connectorState.map(_.asJava).asJava
  }
}
