/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.jakartajms.javadsl

import java.util.concurrent.CompletionStage

import akka.stream.alpakka.jakartajms.{scaladsl, JmsEnvelope, JmsMessage, JmsProducerSettings}
import akka.stream.javadsl.Source
import akka.stream.scaladsl.{Flow, Keep}
import akka.util.ByteString
import akka.{Done, NotUsed}

import scala.jdk.CollectionConverters._
import scala.jdk.FutureConverters._

/**
 * Factory methods to create JMS producers.
 */
object JmsProducer {

  /**
   * Create a flow to send [[akka.stream.alpakka.jakartajms.JmsMessage JmsMessage]] sub-classes to
   * a JMS broker.
   */
  def flow[R <: JmsMessage](
      settings: JmsProducerSettings
  ): akka.stream.javadsl.Flow[R, R, JmsProducerStatus] =
    akka.stream.alpakka.jakartajms.scaladsl.JmsProducer.flow(settings).mapMaterializedValue(toProducerStatus).asJava

  /**
   * Create a flow to send [[akka.stream.alpakka.jakartajms.JmsEnvelope JmsEnvelope]] sub-classes to
   * a JMS broker to support pass-through of data.
   */
  def flexiFlow[PassThrough](
      settings: JmsProducerSettings
  ): akka.stream.javadsl.Flow[JmsEnvelope[PassThrough], JmsEnvelope[PassThrough], JmsProducerStatus] =
    akka.stream.alpakka.jakartajms.scaladsl.JmsProducer
      .flexiFlow[PassThrough](settings)
      .mapMaterializedValue(toProducerStatus)
      .asJava

  /**
   * Create a sink to send [[akka.stream.alpakka.jakartajms.JmsMessage JmsMessage]] sub-classes to
   * a JMS broker.
   */
  def sink[R <: JmsMessage](
      settings: JmsProducerSettings
  ): akka.stream.javadsl.Sink[R, CompletionStage[Done]] =
    akka.stream.alpakka.jakartajms.scaladsl.JmsProducer
      .sink(settings)
      .mapMaterializedValue(_.asJava)
      .asJava

  /**
   * Create a sink to send Strings as text messages to a JMS broker.
   */
  def textSink(settings: JmsProducerSettings): akka.stream.javadsl.Sink[String, CompletionStage[Done]] =
    akka.stream.alpakka.jakartajms.scaladsl.JmsProducer
      .textSink(settings)
      .mapMaterializedValue(_.asJava)
      .asJava

  /**
   * Create a sink to send byte arrays to a JMS broker.
   */
  def bytesSink(settings: JmsProducerSettings): akka.stream.javadsl.Sink[Array[Byte], CompletionStage[Done]] =
    akka.stream.alpakka.jakartajms.scaladsl.JmsProducer
      .bytesSink(settings)
      .mapMaterializedValue(_.asJava)
      .asJava

  /**
   * Create a sink to send [[akka.util.ByteString ByteString]]s to a JMS broker.
   */
  def byteStringSink(settings: JmsProducerSettings): akka.stream.javadsl.Sink[ByteString, CompletionStage[Done]] =
    akka.stream.alpakka.jakartajms.scaladsl.JmsProducer
      .byteStringSink(settings)
      .mapMaterializedValue(_.asJava)
      .asJava

  /**
   * Create a sink to send map structures to a JMS broker.
   */
  def mapSink(
      settings: JmsProducerSettings
  ): akka.stream.javadsl.Sink[java.util.Map[String, Any], CompletionStage[Done]] = {

    val scalaSink =
      akka.stream.alpakka.jakartajms.scaladsl.JmsProducer
        .mapSink(settings)
        .mapMaterializedValue(_.asJava)
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
    akka.stream.alpakka.jakartajms.scaladsl.JmsProducer
      .objectSink(settings)
      .mapMaterializedValue(_.asJava)
      .asJava

  private def toProducerStatus(scalaStatus: scaladsl.JmsProducerStatus) = new JmsProducerStatus {

    override def connectorState: Source[JmsConnectorState, NotUsed] =
      scalaStatus.connectorState.map(_.asJava).asJava
  }
}
