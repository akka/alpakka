/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.jms.javadsl

import java.util.concurrent.CompletionStage

import akka.stream.alpakka.jms.{JmsMessage, JmsSinkSettings, JmsSinkStage}
import akka.stream.scaladsl.{Flow, Keep}
import akka.{Done, NotUsed}

import scala.collection.JavaConversions
import scala.compat.java8.FutureConverters

object JmsSink {

  /**
   * Java API: Creates an [[JmsSink]] for [[JmsMessage]]s
   */
  def create[R <: JmsMessage](jmsSinkSettings: JmsSinkSettings): akka.stream.javadsl.Sink[R, CompletionStage[Done]] =
    akka.stream.scaladsl.Sink
      .fromGraph(new JmsSinkStage(jmsSinkSettings))
      .mapMaterializedValue(FutureConverters.toJava)
      .asJava

  /**
   * Java API: Creates an [[JmsSink]] for strings
   */
  def textSink(jmsSinkSettings: JmsSinkSettings): akka.stream.javadsl.Sink[String, CompletionStage[Done]] =
    akka.stream.alpakka.jms.scaladsl.JmsSink
      .textSink(jmsSinkSettings)
      .mapMaterializedValue(FutureConverters.toJava)
      .asJava

  /**
   * Java API: Creates an [[JmsSink]] for bytes
   */
  def bytesSink(jmsSinkSettings: JmsSinkSettings): akka.stream.javadsl.Sink[Array[Byte], CompletionStage[Done]] =
    akka.stream.alpakka.jms.scaladsl.JmsSink
      .bytesSink(jmsSinkSettings)
      .mapMaterializedValue(FutureConverters.toJava)
      .asJava

  /**
   * Java API: Creates an [[JmsSink]] for maps with primitive datatypes as value
   */
  def mapSink(
      jmsSinkSettings: JmsSinkSettings
  ): akka.stream.javadsl.Sink[java.util.Map[String, Any], CompletionStage[Done]] = {

    val scalaSink =
      akka.stream.alpakka.jms.scaladsl.JmsSink.mapSink(jmsSinkSettings).mapMaterializedValue(FutureConverters.toJava)
    val javaToScalaConversion =
      Flow.fromFunction((javaMap: java.util.Map[String, Any]) => JavaConversions.mapAsScalaMap(javaMap).toMap)
    javaToScalaConversion.toMat(scalaSink)(Keep.right).asJava
  }

  /**
   * Java API: Creates an [[JmsSink]] for serializable objects
   */
  def objectSink(
      jmsSinkSettings: JmsSinkSettings
  ): akka.stream.javadsl.Sink[java.io.Serializable, CompletionStage[Done]] =
    akka.stream.alpakka.jms.scaladsl.JmsSink
      .objectSink(jmsSinkSettings)
      .mapMaterializedValue(FutureConverters.toJava)
      .asJava

}
