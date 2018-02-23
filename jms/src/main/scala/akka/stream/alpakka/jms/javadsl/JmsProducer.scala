/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.jms.javadsl

import java.util.concurrent.CompletionStage

import akka.{Done, NotUsed}
import akka.stream.alpakka.jms.{JmsMessage, JmsProducerSettings, JmsProducerStage}
import akka.stream.scaladsl.{Flow, Keep}

import scala.collection.JavaConversions
import scala.compat.java8.FutureConverters

object JmsProducer {

  /**
   * Java API: Creates an [[JmsProducer]] for [[JmsMessage]]s
   */
  def flow[R <: JmsMessage](
      settings: JmsProducerSettings
  ): akka.stream.javadsl.Flow[R, R, NotUsed] =
    akka.stream.alpakka.jms.scaladsl.JmsProducer.flow(settings).asJava

  /**
   * Java API: Creates an [[JmsProducer]] for [[JmsMessage]]s
   */
  def create[R <: JmsMessage](
      settings: JmsProducerSettings
  ): akka.stream.javadsl.Sink[R, CompletionStage[Done]] =
    akka.stream.alpakka.jms.scaladsl.JmsProducer
      .apply(settings)
      .mapMaterializedValue(FutureConverters.toJava)
      .asJava

  /**
   * Java API: Creates an [[JmsProducer]] for strings
   */
  def textSink(settings: JmsProducerSettings): akka.stream.javadsl.Sink[String, CompletionStage[Done]] =
    akka.stream.alpakka.jms.scaladsl.JmsProducer
      .textSink(settings)
      .mapMaterializedValue(FutureConverters.toJava)
      .asJava

  /**
   * Java API: Creates an [[JmsProducer]] for bytes
   */
  def bytesSink(settings: JmsProducerSettings): akka.stream.javadsl.Sink[Array[Byte], CompletionStage[Done]] =
    akka.stream.alpakka.jms.scaladsl.JmsProducer
      .bytesSink(settings)
      .mapMaterializedValue(FutureConverters.toJava)
      .asJava

  /**
   * Java API: Creates an [[JmsProducer]] for maps with primitive datatypes as value
   */
  def mapSink(
      settings: JmsProducerSettings
  ): akka.stream.javadsl.Sink[java.util.Map[String, Any], CompletionStage[Done]] = {

    val scalaSink =
      akka.stream.alpakka.jms.scaladsl.JmsProducer
        .mapSink(settings)
        .mapMaterializedValue(FutureConverters.toJava)
    val javaToScalaConversion =
      Flow.fromFunction((javaMap: java.util.Map[String, Any]) => JavaConversions.mapAsScalaMap(javaMap).toMap)
    javaToScalaConversion.toMat(scalaSink)(Keep.right).asJava
  }

  /**
   * Java API: Creates an [[JmsProducer]] for serializable objects
   */
  def objectSink(
      settings: JmsProducerSettings
  ): akka.stream.javadsl.Sink[java.io.Serializable, CompletionStage[Done]] =
    akka.stream.alpakka.jms.scaladsl.JmsProducer
      .objectSink(settings)
      .mapMaterializedValue(FutureConverters.toJava)
      .asJava

}
