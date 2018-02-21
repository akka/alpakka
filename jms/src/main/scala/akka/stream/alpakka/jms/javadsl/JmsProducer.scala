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
      jmsSinkSettings: JmsProducerSettings
  ): akka.stream.javadsl.Flow[R, JmsMessage, NotUsed] =
    akka.stream.alpakka.jms.scaladsl.JmsProducer.flow(jmsSinkSettings).asJava

  /**
   * Java API: Creates an [[JmsProducer]] for [[JmsMessage]]s
   */
  def create[R <: JmsMessage](
      jmsSinkSettings: JmsProducerSettings
  ): akka.stream.javadsl.Sink[R, CompletionStage[Done]] =
    akka.stream.alpakka.jms.scaladsl.JmsProducer
      .apply(jmsSinkSettings)
      .mapMaterializedValue(FutureConverters.toJava)
      .asJava

  /**
   * Java API: Creates an [[JmsProducer]] for strings
   */
  def textSink(jmsSinkSettings: JmsProducerSettings): akka.stream.javadsl.Sink[String, CompletionStage[Done]] =
    akka.stream.alpakka.jms.scaladsl.JmsProducer
      .textSink(jmsSinkSettings)
      .mapMaterializedValue(FutureConverters.toJava)
      .asJava

  /**
   * Java API: Creates an [[JmsProducer]] for bytes
   */
  def bytesSink(jmsSinkSettings: JmsProducerSettings): akka.stream.javadsl.Sink[Array[Byte], CompletionStage[Done]] =
    akka.stream.alpakka.jms.scaladsl.JmsProducer
      .bytesSink(jmsSinkSettings)
      .mapMaterializedValue(FutureConverters.toJava)
      .asJava

  /**
   * Java API: Creates an [[JmsProducer]] for maps with primitive datatypes as value
   */
  def mapSink(
      jmsSinkSettings: JmsProducerSettings
  ): akka.stream.javadsl.Sink[java.util.Map[String, Any], CompletionStage[Done]] = {

    val scalaSink =
      akka.stream.alpakka.jms.scaladsl.JmsProducer
        .mapSink(jmsSinkSettings)
        .mapMaterializedValue(FutureConverters.toJava)
    val javaToScalaConversion =
      Flow.fromFunction((javaMap: java.util.Map[String, Any]) => JavaConversions.mapAsScalaMap(javaMap).toMap)
    javaToScalaConversion.toMat(scalaSink)(Keep.right).asJava
  }

  /**
   * Java API: Creates an [[JmsProducer]] for serializable objects
   */
  def objectSink(
      jmsSinkSettings: JmsProducerSettings
  ): akka.stream.javadsl.Sink[java.io.Serializable, CompletionStage[Done]] =
    akka.stream.alpakka.jms.scaladsl.JmsProducer
      .objectSink(jmsSinkSettings)
      .mapMaterializedValue(FutureConverters.toJava)
      .asJava

}
